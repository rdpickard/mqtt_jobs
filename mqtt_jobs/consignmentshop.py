from mqtt_jobs.consignmentshop_message_schemas import *

import logging
import logging.handlers
import sys
import threading
import traceback
import base64
import re
import secrets
import time
import json
import datetime

import paho.mqtt.client as mqtt

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

import jsonschema

Base = declarative_base()


def gen_hex_id(prefix: str = "", length: int = 22):
    """
    Create a string of random hex values. Can be used to make id values
    :param prefix: Add a prefix to the string. Defaults to no prefix
    :param length: The length of the string
    :return: prefix (if any) + hex string as str
    """
    return "{prefix}{random_val}".format(prefix=prefix, random_val=secrets.token_hex(length))


def python_fstring_to_regex(fstring):
    """
    Create a regex with named match groups from a python format string with named parameters.

    For example "Group name is {group_name}" -> 'Group name is (?P<group_name>.[a-zA-Z0-9]*)'

    NOTE: The returned regex will only match on [a-zA-Z0-9]
    :param fstring: String to create regex from
    :return: Regex as a string
    """
    return re.sub(r'{([a-zA-Z0-9_]*)}', r'(?P<\1>.[a-zA-Z0-9]*)', fstring)


class ConsignmentClientException(Exception):
    """
    Wrapper class of Exception. Adds no functionality, only for better try/catch behavior
    """
    pass


class ConsignmentThreadedTaskManager:
    """
    Maps ConsignmentTask objects to a task name for access. Tasks are 'registered' with the TaskManager
    through the add_task method with a name. Registered Tasks can then be accessed by name.

    The incoming_task_contract method is a convenience method for workers. It executes a specified
    ConsignmentTask.task and takes care of publishing the results on the right MQTT topic
    """

    _tasks = None
    _logger = None

    def __init__(self, logger):
        """
        New task manager instance. Created with no registered tasks. Tasks must be added with add_task.

        :param logger: The logger to use. Most likely the ConsignmentShopMQTTThreadedClient.logger for the worker
        using this manager instance is the appropriate value.
        """
        self._tasks = dict()
        self._logger = logger
        _do_debug = False

    def add_task(self, name: str, task):
        """
        Register a ConsignmentTask class with this manager by a name.

        Note: The 'task' value is a class not an instance of the class

        Example: manager.add_task("count", CountTask)

        :param name: The name the task will be referred to as. If there a is already a registered ConsignmentTask with
        that name, the old value will be overwritten
        :param task: Class of ConsignmentTask that is associated to the name
        :return: Nothing
        """

        # TODO Check that passed in task is a class, not an instance
        if name in self.tasks_available():
            self._logger.warning("Overriding task '{name}'".format(name=name))
        else:
            self._logger.info("Adding task '{name}'".format(name=name))

        self._tasks[name] = task

    def task_for_name(self, name):
        """
        Access ConsignmentTask class by name
        :param name: Name of task. Case sensitive, exact match
        :return: ConsignmentTask or None if there is no match for the name
        """
        return self._tasks.get(name, None)

    def tasks_available(self):
        """
        A list of the names of registered tasks
        :return: list of strings
        """
        return list(self._tasks.keys())

    def incoming_task_contract(self, contract_msg, shop_client, logger):
        """
        A convenience method that works as a callback for
        ConsignmentShopMQTTThreadedClient.subscribe_to_topic_with_callback.

        Takes a "Consignment Contract Message" JSON message (see consignment_shop_schemas for message structure) and
        invokes the 'task' function of the ConsignmentTask that is registered with the 'task_name' parameter of
        the JSON message.

        The yielded or returned results for the ConsignmentTask.task method are published to the MQTT topic that is
        appropriate for the Consignment that the contract message is associated with. ConsignmentResult.publish_result
        is called to do the actual work of sending the MQTT message.

        It is the "Invoker" for the "Command Pattern" of how tasks are executed by the worker based on instructions from
        the Consignment keeper. The ConsignmentTask is the "Command", the ConsignmentShopMQTTThreadedClient is both the
        "Client" and "Receiver" of the "Command Pattern".

        :param contract_msg: JSON "Consignment Contract Message" as a string. If value does not validate against the
        message schema the message will be ignored and there will be a logging message at 'warning' level
        :param shop_client: The ConsignmentShopMQTTThreadedClient that received the contract_message
        :param logger: The logger to use. DEPRECATED the shop_client has a logger method that is used
        """

        # TODO Remove deprecated logger parameter

        try:
            payload = json.loads(contract_msg.payload)
            jsonschema.validate(instance=payload, schema=msg_json_schema_contract)
        except json.decoder.JSONDecodeError:
            logger.warning("Offer response message payload isn't json. Ignoring message.")
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=contract_msg.topic,
                                                                                         payload=contract_msg.payload))
            return

        if payload['task_name'] not in self.tasks_available():
            # TODO Add feedback to the keeper that this contract can't be honored
            logger.warning("Task '{task_name}' isn't available for the worker. Ignoring contract message".format(
                task_name=payload['task_name']))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=contract_msg.topic,
                                                                                         payload=contract_msg.payload))
            return

        logger.info("New contract!")
        # TODO Task execution should be in it's own thread

        for seq, res in self.task_for_name(payload['task_name']).task(shop_client, payload['task_parameters']):
            ConsignmentResult.publish_result(shop_client, res, payload['offer_id'], payload['consignment_id'], seq)

        # Send results that the worker is finished
        ConsignmentResult.publish_result(shop_client, None, payload['offer_id'], payload['consignment_id'], 0, True)


class ConsignmentShop:
    """
    Bundles together parameter values, such at MQTT topic paths, and factory methods to create the worker and keeper
    actors that implement the Consignment methodologies.

    The worker and keeper ConsignmentShopMQTTThreadedClient instances that are returned from the factory methods a
    common instance of ConsignmentShop well be configured to communicate among themselves on the same MQTT topics.
    The workers will be sending heartbeats on the topic the keeper will be listening for them.

    The workers will be subscribed to the MQTT topic where the keeper posts new ConsignmentOffers. Each worker is
    subscribed to it's own MQTT topic for incoming ConsignmentContracts.

    The keeper, in addition to listening for worker heartbeats, is subscribed to all MQTT topics where workers
    will publish the results of running ConsignmentTasks.

    Both workers and keeper subscriptions will be configured to call back to handle incoming messages.

    Keeper subscriptions and call backs:
    ConsignmentShop.topic_heartbeats -> ConsignmentWorkerShadow.incoming_heartbeat
    ConsignmentShop.topic_keeper_accepted_offer -> ConsignmentOffer.incoming_offer_response
    ConsignmentShop.topic_consignment_results -> ConsignmentResult.incoming_result

    Worker subscriptions and call backs:
    ConsignmentShop.topic_offers_dispatch -> ConsignmentOffer.incoming_offer
    ConsignmentShop.topic_worker_contracts -> ConsignmentThreadedTaskManager.incoming_task_contract
    """

    _name = None
    _tasks = None
    _logger = None
    _do_debug = False
    _mqtt_broker_port = None
    _mqtt_broker_host = None

    topic_offers_dispatch = "mqtt/workers/offers/dispatch"
    topic_worker_contracts = "mqtt/workers/{client_id}/contracts"
    topic_keeper_accepted_offer = "mqtt/keeper/offers/inbox"
    topic_consignment_results = "mqtt/keeper/consignment/{consignment_id}/results"
    topic_heartbeats = "mqtt/keeper/heartbeats"

    # TODO MQTT paths need to support a prefix + a shoppe name level in the path for differentiation

    def __init__(self, mqtt_broker_host, mqtt_broker_port=1883, do_debug=False):
        self._mqtt_broker_host = mqtt_broker_host
        self._mqtt_broker_port = mqtt_broker_port
        self._do_debug = do_debug

    @property
    def name(self):
        """
        The name of this shop.

        :return: string
        """
        return self._name

    @property
    def do_debug(self):
        """
        If the logging for ConsignmentShopMQTTThreadedClient created by factories should log at the most verbose
        :return: boolean
        """
        return self._do_debug

    @do_debug.setter
    def do_debug(self, do):
        """
        Set if the logging for ConsignmentShopMQTTThreadedClient created by factories should log at the most verbose
        """
        self._do_debug = do

    def consignment_worker_factory(self, client_id, tasks):
        """
        Create ConsignmentShopMQTTThreadedClient that is preconfigured to act as a worker

        :param client_id: The identification of the client. Must be unique among all clients
        :param tasks: Dictionary keyed on task name mapped to ConsignmentTask classes. Used to create
        ConsignmentTaskManager
        :return: ConsignmentShopMQTTThreadedClient
        """
        # TODO need to add authentication for MQTT broker
        client = ConsignmentShopMQTTThreadedClient(client_id, self._mqtt_broker_host, self._mqtt_broker_port)
        if self.do_debug:
            client.logger.setLevel(logging.DEBUG)
        else:
            client.logger.setLevel(logging.INFO)

        task_manager = ConsignmentThreadedTaskManager(client.logger)

        for task_name, task_class in tasks.items():
            task_manager.add_task(task_name, task_class)

        # Listen for new offers
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_offers_dispatch,
                                                ConsignmentOffer.incoming_offer)

        # Listen for contracts to offers I bid for
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_worker_contracts.format(client_id=client_id),
                                                task_manager.incoming_task_contract)

        client.send_heartbeat()
        client.start()

        client.start_sending_heartbeats(1)

        return client

    def consignment_keeper_factory(self, client_id, tasks, db_uri, db_echo=False):
        """
        Create ConsignmentShopMQTTThreadedClient that is preconfigured to act as a keeper

        :param client_id: The identification of the keeper
        :param tasks: Dictionary keyed on task name mapped to ConsignmentTask classes. Used to create
        ConsignmentTaskManager
        :param db_uri: The URI for creating a sqlalchemy engine for DB access
        :param db_echo: Enable / disable verbose sqlalchemy logging of SQL commands. Defaults to False
        :return: ConsignmentShopMQTTThreadedClient
        :raises TimeoutErr: If MQTT server can't be reached
        """

        # TODO need to add authentication for MQTT broker


        client = ConsignmentShopMQTTThreadedClient(client_id, self._mqtt_broker_host, self._mqtt_broker_port)
        if self.do_debug:
            client.logger.setLevel(logging.DEBUG)
        else:
            client.logger.setLevel(logging.INFO)

        client.task_manager = ConsignmentThreadedTaskManager(client.logger)
        for task_name, task_class in tasks.items():
            client.task_manager.add_task(task_name, task_class)

        db_engine = sqlalchemy.create_engine(db_uri, echo=db_echo)
        Base.metadata.create_all(db_engine)
        client.db_session_maker = sqlalchemy.orm.sessionmaker(bind=db_engine)

        # P listen for workers accepting offers
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_keeper_accepted_offer,
                                                ConsignmentOffer.incoming_offer_response)

        # P listen for worker heartbeats
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_heartbeats,
                                                ConsignmentWorkerShadow.incoming_heartbeat)

        # P listen for work results
        client.subscribe_to_topic_with_callback(
            ConsignmentShop.topic_consignment_results.format(consignment_id="([a-zA-Z0-9]*)"),
            ConsignmentResult.incoming_result)

        client.start()

        return client


class Consignment(Base):
    """
    The instigator for workers ultimately executing tasks and reporting the results.

    Describes the type of task to be done by workers, the characteristics a worker needs to have to do the task
    correctly, how many of those workers are required to have a sufficient data set of results, the particulars of
    what to apply the task to and when consider the task to have been completed satisfactorily or unsatisfactorily.

    The class is SQLAlchemy Base object to persist instances to a DB.
    """

    __tablename__ = "consignment"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    description = sqlalchemy.Column(sqlalchemy.String)
    descriptive_details = sqlalchemy.Column(sqlalchemy.PickleType)
    # TODO add descriptive tags to Consignment

    task_name = sqlalchemy.Column(sqlalchemy.String)
    task_parameters = sqlalchemy.Column(sqlalchemy.PickleType)
    worker_parameters = sqlalchemy.Column(sqlalchemy.PickleType)

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow())
    closed_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    last_updated_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    ttl_in_seconds = sqlalchemy.Column(sqlalchemy.Integer, default=60)

    STATE_NEW = "NEW"
    STATE_ACTIVE = "ACTIVE"
    STATE_PARTIAL = "PARTIAL"
    STATE_HEALTHY = "HEALTHY"
    STATE_COMPLETED = "COMPLETED"
    STATE_ABANDONED = "ABANDONED"
    state = sqlalchemy.Column(sqlalchemy.String, default=STATE_NEW)
    state_when_closed = sqlalchemy.Column(sqlalchemy.String)

    results = sqlalchemy.orm.relationship("ConsignmentResult", back_populates="consignment")
    offers = sqlalchemy.orm.relationship("ConsignmentOffer", back_populates="consignment")

    healthy_pattern = sqlalchemy.Column(sqlalchemy.String)
    completed_pattern = sqlalchemy.Column(sqlalchemy.String)

    @staticmethod
    def new_consignment(shop_client, name: str, task_name: str, task_parameters: dict, worker_parameters: dict,
                        description: str = None, descriptive_details: dict = None):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new Consignment client has no db session maker")

        consignment = Consignment()
        consignment.id = gen_hex_id("C")
        consignment.name = name
        consignment.task_name = task_name
        consignment.task_parameters = task_parameters
        consignment.worker_parameters = worker_parameters
        consignment.description = description
        consignment.descriptive_details = descriptive_details

        db_session = shop_client.db_session_maker()
        db_session.add(consignment)
        db_session.flush()
        db_session.commit()

        shop_client.subscribe_to_topic(ConsignmentShop.topic_consignment_results.format(consignment_id=consignment.id))
        db_session.close()

        return consignment

    def close(self, shop_client):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")

        db_session = shop_client.db_session_maker()
        self.closed_timestamp_utc = datetime.datetime.utcnow()
        self.state_when_closed = self.state
        self.state = self.STATE_COMPLETED
        db_session.flush()
        db_session.commit()

        for offer in self.offers:
            offer.close()

        return

    def make_new_offer(self, shop_client, offer_description="opportunity awaits from the top!"):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")
        if self.state == self.STATE_COMPLETED or self.state == self.STATE_ABANDONED:
            raise ConsignmentClientException(
                "Can't make new offer consignment '{id}' was closed at {closed} in state '{state}'".format(
                    id=self.id, closed=self.closed_timestamp_utc, state=self.state
                ))

        offer = ConsignmentOffer.new_offer_for_consignment(shop_client, self.id, offer_description)

        db_session = shop_client.db_session_maker()
        if self.state == self.STATE_NEW:
            self.state = self.STATE_ACTIVE
        self.last_updated_timestamp_utc = datetime.datetime.utcnow()
        db_session.flush()
        db_session.commit()

        shop_client.task_manager.task_for_name(self.task_name).on_consignment_open(shop_client, self)

        return offer

    @staticmethod
    def is_healthy_when(consignment_id, shop_client, pattern):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")

        db_session = shop_client.db_session_maker()
        consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()
        if consignment is None:
            raise ConsignmentClientException(
                "Could not set healthy pattern because there is no Consignment in the DB with id '{id}'".format(
                    id=consignment_id))

        consignment.healthy_pattern = pattern
        db_session.flush()
        db_session.commit()
        db_session.close()

    @staticmethod
    def is_completed_when(consignment_id, shop_client, pattern):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")

        db_session = shop_client.db_session_maker()
        consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()
        if consignment is None:
            raise ConsignmentClientException(
                "Could not set completed pattern because there is no Consignment in the DB with id '{id}'".format(
                    id=consignment_id))

        db_session = shop_client.db_session_maker()
        consignment.completed_pattern = pattern
        db_session.flush()
        db_session.commit()
        db_session.close()


class ConsignmentOffer(Base):
    __tablename__ = "consignment_offer"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    consignment_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment.id'))
    consignment = sqlalchemy.orm.relationship("Consignment", back_populates="offers")
    description = sqlalchemy.Column(sqlalchemy.String, default="opportunity awaits")
    contracts = sqlalchemy.orm.relationship("ConsignmentContract")

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow())
    opened_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    closed_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    STATE_LATENT = "LATENT"
    STATE_OPEN = "OPEN"
    STATE_CLOSED = "CLOSED"
    state = sqlalchemy.Column(sqlalchemy.String, default=STATE_LATENT)

    def close(self, shop_client):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't close offer, client has no db session maker")
        db_session = shop_client.db_session_maker()
        self.closed_timestamp_utc = datetime.datetime.utcnow()
        self.state = self.STATE_CLOSED
        db_session.flush()
        db_session.commit()
        return

    @staticmethod
    def new_offer_for_consignment(shop_client, consignment_id, description="opportunity awaits", open_and_publish=True):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")

        db_session = shop_client.db_session_maker()

        consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()
        if consignment is None:
            raise ConsignmentClientException(
                "Can't make new offer for consignment, not consignment with id '{id}' in DB".format(id=consignment_id))

        if consignment.state == Consignment.STATE_COMPLETED or consignment.state == Consignment.STATE_ABANDONED:
            shop_client.logger.warning(
                "Creating an offer for consignment '{id}' which is finished".format(id=consignment.id))

        offer = ConsignmentOffer()
        offer.id = gen_hex_id("O")
        offer.consignment_id = consignment.id
        offer.description = description

        if open_and_publish:
            offer.state = ConsignmentOffer.STATE_OPEN

        db_session.add(offer)
        db_session.flush()
        db_session.commit()

        if open_and_publish:
            shop_client.publish_on_topic(ConsignmentShop.topic_offers_dispatch,
                                         ConsignmentOffer.dumps_offer_message(offer))

        return offer

    @staticmethod
    def dumps_offer_message(consignment_offer):
        msg = {
            "offer_id": consignment_offer.id,
            "task_name": consignment_offer.consignment.task_name,
            "worker_parameters": consignment_offer.consignment.worker_parameters
        }
        return json.dumps(msg)

    @staticmethod
    def dumps_offer_response_message(consignment_offer_id, shop_client):
        msg = {
            "offer_id": consignment_offer_id,
            "client_id": shop_client.client_id,
        }
        return json.dumps(msg)

    @staticmethod
    def incoming_offer(offer_msg, shop_client, logger):
        try:
            payload = json.loads(offer_msg.payload)
            jsonschema.validate(instance=payload, schema=msg_json_schema_offer)
        except json.decoder.JSONDecodeError:
            logger.warning("Offer message payload isn't json. Ignoring message.")
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_msg.topic,
                                                                                         payload=offer_msg.payload))
            return
        except jsonschema.ValidationError as ve:
            logger.warning("Offer message payload failed validation. Ignoring message.")
            logger.debug("Ignored message validation err '{err}'".format(err=ve))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_msg.topic,
                                                                                         payload=str(
                                                                                             offer_msg.payload)))
            return

        if shop_client.assess_offer_needs(payload['worker_parameters']):
            # TODO Revisit having to send a heart beat before accepting an offer to avoid worker_shadow race condition
            # A race condition exists when the keeper makes a new offer but has not received a heartbeat message from
            # a client that will accept the offer. If the keeper has not seen a heartbeat from the client then there will
            # be no worker_shadow entry in the DB for that client and the keeper will reject the offer to execute the
            # offer. This is likely only a problem in the initial testing I am doing for development where the DB is
            # getting tossed every run, but it's better to address the race condition
            shop_client.send_heartbeat()
            shop_client.publish_on_topic(ConsignmentShop.topic_keeper_accepted_offer,
                                         ConsignmentOffer.dumps_offer_response_message(payload['offer_id'],
                                                                                       shop_client))

    @staticmethod
    def incoming_offer_response(offer_response_msg, shop_client, logger):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")
        try:
            payload = json.loads(offer_response_msg.payload)
            jsonschema.validate(instance=payload, schema=msg_json_schema_offer_response)
        except json.decoder.JSONDecodeError:
            logger.warning("Offer response message payload isn't json. Ignoring message.")
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic,
                                                                                         payload=offer_response_msg.payload))
            return
        except jsonschema.ValidationError as ve:
            logger.warning("Offer response message payload failed validation. Ignoring message.")
            logger.debug("Ignored message validation err '{err}'".format(err=ve))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic,
                                                                                         payload=str(
                                                                                             offer_response_msg.payload)))
            return

        db_session = shop_client.db_session_maker()

        # P Check that the offer exists and is in a state to consider new contract
        offer = db_session.query(ConsignmentOffer).filter_by(id=payload['offer_id']).one_or_none()
        if offer is None:
            logger.warning("No consignment offer with id \'{id}\' in DB. Ignoring message.".format(
                id=payload["offer_id"]))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic,
                                                                                         payload=json.dumps(payload)))
            return
        if offer.state != ConsignmentOffer.STATE_OPEN:
            logger.warning("Offer \'{id}\' is not '{open}' is '{state}'. Ignoring message.".format(
                id=offer.id, open=ConsignmentOffer.STATE_OPEN, state=offer.state))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic,
                                                                                         payload=json.dumps(payload)))
            return

        if not eval_consignment_characteristic(offer.consignment.healthy_pattern, offer.consignment_id,
                                               shop_client.db_session_maker, shop_client.logger):
            # P The consignment wants more workers, send the worker a contract
            contract_id = ConsignmentContract.new_contract_for_client_for_consignment(shop_client, offer.id,
                                                                                      payload['client_id'])
            if contract_id is None:
                logger.error(
                    "Could not create a contract with client '{cid}' for offer '{id}'".format(cid=payload['client_id'],
                                                                                              id=offer.id))
                return
            shop_client.publish_on_topic(ConsignmentShop.topic_worker_contracts.format(client_id=payload['client_id']),
                                         ConsignmentContract.dumps_contract_message(contract_id, shop_client))


class ConsignmentContract(Base):
    __tablename__ = "consignment_contract"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    offer_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_offer.id'))
    offer = sqlalchemy.orm.relationship("ConsignmentOffer", back_populates="contracts")
    worker_shadow_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_worker_shadow.id'))
    consignment_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment.id'))

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow())
    sent_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    closed_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    STATE_OPEN = "OPEN"
    STATE_CLOSED = "CLOSED"
    state = sqlalchemy.Column(sqlalchemy.String, default=STATE_OPEN)

    @staticmethod
    def dumps_contract_message(contract_id, shop_client):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't dump contract message client has no db session maker")

        db_session = shop_client.db_session_maker()
        contract = db_session.query(ConsignmentContract).filter(ConsignmentContract.id == contract_id).one_or_none()
        if contract is None:
            raise ConsignmentClientException(
                "Can't dump contract message client no contract with id '{}' in DB".format(contract_id))

        msg = {
            "consignment_id": contract.consignment_id,
            "offer_id": contract.offer.id,
            "contract_id": contract.id,
            "task_name": contract.offer.consignment.task_name,
            "task_parameters": contract.offer.consignment.task_parameters
        }
        db_session.close()
        return json.dumps(msg)

    @staticmethod
    def incoming_contract(contract_msg, shop_client, logger):
        pass

    @staticmethod
    def new_contract_for_client_for_consignment(shop_client, offer_id, client_id):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")

        db_session = shop_client.db_session_maker()

        # P get the shadow of the worker that is reporting the results
        shadow = db_session.query(ConsignmentWorkerShadow).filter_by(client_id=client_id).one_or_none()
        if shadow is None:
            shop_client.logger.warning(
                "No worker shadow for with client id \'{id}\' in DB. Ignoring contract request.".format(id=client_id))
            return None
        offer = db_session.query(ConsignmentOffer).filter_by(id=offer_id).one_or_none()
        if offer is None:
            shop_client.logger.warning(
                "No offer for with id \'{id}\' in DB. Ignoring contract request.".format(id=offer_id))
            return None

        new_contract_id = gen_hex_id("XT")
        contract = ConsignmentContract()
        contract.id = new_contract_id
        contract.offer_id = offer.id
        contract.offer = offer
        contract.worker_shadow_id = shadow.id
        contract.consignment_id = offer.consignment_id
        contract.closed_timestamp_utc = None
        db_session.flush()
        db_session.commit()

        db_session.close()

        return new_contract_id


class ConsignmentResult(Base):
    __tablename__ = "consignment_result"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow())
    worker_sent_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    consignment_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment.id'))
    consignment = sqlalchemy.orm.relationship("Consignment", back_populates="results")
    offer_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_offer.id'))
    worker_shadow_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_worker_shadow.id'))
    worker_shadow = sqlalchemy.orm.relationship("ConsignmentWorkerShadow", back_populates="results")

    results = sqlalchemy.Column(sqlalchemy.String)
    results_encoding = sqlalchemy.Column(sqlalchemy.String)
    work_sequence_number = sqlalchemy.Column(sqlalchemy.String)

    worker_finished = sqlalchemy.Column(sqlalchemy.Boolean)

    # TODO Add HMAC support for signed results

    @staticmethod
    def incoming_result(result_msg, shop_client, logger):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't create worker shadow, provided mqtt_client has no db session maker")
        try:
            payload = json.loads(result_msg.payload)
            jsonschema.validate(instance=payload, schema=msg_json_schema_task_results)
        except json.decoder.JSONDecodeError:
            logger.warning("Results message payload isn't json. Ignoring message.")
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                         payload=str(
                                                                                             result_msg.payload)))
            return
        except jsonschema.ValidationError as ve:
            logger.warning("Results message payload failed validation. Ignoring message.")
            logger.debug("Ignored message validation err '{err}'".format(err=ve))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                         payload=str(
                                                                                             result_msg.payload)))
            return

        db_session = shop_client.db_session_maker()
        try:

            # P Get the offer that the worker signed up to
            offer = db_session.query(ConsignmentOffer).filter_by(id=payload['offer_id']).one_or_none()
            if offer is None:
                logger.warning("No consignment offer with id \'{id}\' in DB. Ignoring message.".format(
                    id=payload["offer_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return
            if offer.state != ConsignmentOffer.STATE_OPEN:
                logger.warning("Offer \'{id}\' is not '{open}' is '{state}'. Ignoring message.".format(
                    id=offer.id, open=ConsignmentOffer.STATE_OPEN, state=offer.state))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))

            # MAYBE make sure the worker has a valid contract to do this work

            # P Make sure the consignment exists, is still open and the topic, and offer match
            consignment_id_match = re.match(python_fstring_to_regex(ConsignmentShop.topic_consignment_results),
                                            result_msg.topic)
            if consignment_id_match is None or consignment_id_match.group('consignment_id') is None:
                logger.warning("Could not find consignment id in msg topic {topic} Ignoring message.".format(
                    topic=result_msg.topic))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return
            consignment_id = consignment_id_match.group('consignment_id')
            if consignment_id != payload['consignment_id']:
                logger.warning("Topic '{tid}' and message consignment id '{mid}' don't match Ignoring message.".format(
                    tid=consignment_id, mid=payload['consignment_id']))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return

            consignment = db_session.query(Consignment).filter_by(id=payload['consignment_id']).one_or_none()
            if consignment is None:
                logger.warning("No consignment  with id \'{id}\' in DB. Ignoring message.".format(
                    id=payload["consignment_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return
            if offer.consignment_id != consignment.id:
                logger.warning(
                    "Offer \'{oid}\' has different consignment id of '{oc_id}' than the consignment id of the topic {tc_id}. Ignoring message".format(
                        oid=offer.id, oc_id=offer.consignment_id, tc_id=consignment.id))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return

            now_now = datetime.datetime.utcnow()
            if consignment.last_updated_timestamp_utc is None or now_now > consignment.last_updated_timestamp_utc:
                consignment.last_updated_timestamp_utc = now_now
            db_session.flush()

            # P get the shadow of the worker that is reporting the results
            shadow = db_session.query(ConsignmentWorkerShadow).filter_by(client_id=payload['client_id']).one_or_none()
            if shadow is None:
                logger.warning("No worker shadow for with client id \'{id}\' in DB. Ignoring message.".format(
                    id=payload["client_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                             payload=json.dumps(
                                                                                                 payload)))
                return
            shadow.last_seen_timestamp_utc = datetime.datetime.utcnow()
            db_session.flush()

            result = ConsignmentResult()
            result.id = gen_hex_id("R", 22)
            result.worker_shadow_id = shadow.id
            result.worker_sent_timestamp_utc = datetime.datetime.fromisoformat(payload["sent_timestamp_utc"])
            result.created_timestamp_utc = datetime.datetime.utcnow()
            result.offer_id = payload["offer_id"]
            result.results = payload["results"]
            result.results_encoding = payload["results_encoding"]
            result.work_sequence_number = payload["work_sequence"]
            result.worker_finished = payload["finished"]
            result.consignment = consignment

            db_session.flush()
            db_session.commit()

            # logger.critical("Stored result!")
            # TODO Check consignment on what should be done when a result is stored (task callbacks)

        except sqlalchemy.orm.exc.MultipleResultsFound:
            logger.warning("More than one worker shadow in DB with client id \'{id}\'".format(id=payload["client_id"]))
            db_session.rollback()

    @staticmethod
    def publish_result(shop_client, results, offer_id: str, consignment_id: str, work_sequence: int, finished=False):

        encoding_guess = ""
        encoded_results = None

        if results is None:
            encoded_results = None
            encoding_guess = None
        elif type(results) is str:
            encoded_results = results
            encoding_guess = None
        elif type(results) is dict:
            encoded_results = json.dumps(results)
            encoding_guess = "json"
        elif type(results) is not str:
            encoded_results = str(results)
            encoding_guess = "str"

        result_msg = {
            "client_id": shop_client.client_id,
            "offer_id": offer_id,
            "consignment_id": consignment_id,
            "results": encoded_results,
            "results_encoding": encoding_guess,
            "work_sequence": work_sequence,
            "finished": finished,
            "sent_timestamp_utc": str(datetime.datetime.utcnow())
        }

        shop_client.publish_on_topic(ConsignmentShop.topic_consignment_results.format(consignment_id=consignment_id),
                                     json.dumps(result_msg))


class ConsignmentWorkerShadow(Base):
    __tablename__ = "consignment_worker_shadow"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    description = sqlalchemy.Column(sqlalchemy.PickleType)
    previous_description = sqlalchemy.Column(sqlalchemy.PickleType)
    client_id = sqlalchemy.Column(sqlalchemy.String)

    first_seen_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    last_seen_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    last_description_update_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    results = sqlalchemy.orm.relationship("ConsignmentResult")

    @staticmethod
    def incoming_heartbeat(heartbeat_msg, shop_client, logger):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't create worker shadow, provided mqtt_client has no db session maker")
        try:
            payload = json.loads(heartbeat_msg.payload)
            jsonschema.validate(instance=payload, schema=msg_json_schema_heartbeat)
        except json.decoder.JSONDecodeError:
            raise ConsignmentClientException("Can't create worker shadow, provided msg payload is not JSON")
        except jsonschema.ValidationError as ve:
            raise ConsignmentClientException("Heartbeat message failed schema validation {err}".format(err=str(ve)))

        logger.debug("💖 " + payload['client_id'])

        db_session = shop_client.db_session_maker()
        try:
            shadow = db_session.query(ConsignmentWorkerShadow).filter_by(client_id=payload['client_id']).one_or_none()
            if shadow is None:
                logger.debug("First heartbeat from client \'{client_id}\'".format(client_id=payload["client_id"]))
                shadow = ConsignmentWorkerShadow()
                shadow.id = gen_hex_id("WS", 22)
                shadow.client_id = payload['client_id']
                shadow.first_seen = datetime.datetime.utcnow()
                shadow.description = payload['description']
                db_session.add(shadow)
                db_session.flush()

            if shadow.description is None or shadow.description != payload['description']:
                shadow.previous_description = shadow.description
                shadow.last_description_update_timestamp_utc = datetime.datetime.utcnow()
            shadow.last_seen_timestamp_utc = datetime.datetime.utcnow()

            db_session.flush()
            db_session.commit()

        except sqlalchemy.orm.exc.MultipleResultsFound:
            logger.warning("More than one worker shadow in DB with client id \'{id}\'".format(id=payload["client_id"]))


def client_describe_network(shop_client):
    return {}


def client_describe_host(shop_client):
    return {}


def client_describe_workload(shop_client):
    return {}

def mqtt_threaded_client_exception_catcher(func):

    def wrapper(*args):
        try:
            return func(*args)
        except Exception as e:
            tb = traceback.format_exc()
            logger = args[0].logger
            logger.error("Uncaught exception {function} \"{error}\" \"{tb}\"".format(
                function=str(func),
                error=str(e),
                tb=str(
                    base64.b64encode(
                        tb.encode("utf-8")),
                    "utf-8")))
            logger.debug(tb)

    return wrapper

def client_describe_software(shop_client):
    return {}


class ConsignmentShopMQTTThreadedClient(threading.Thread):
    _client_id = None
    _mqtt_client = None
    _mqtt_connected = False
    _mqtt_broker_host = None
    _mqtt_broker_port = None

    _topic_callbacks = None
    _descriptor_callbacks = None

    _heartbeat_thread = None
    _keep_sending_heart_beats = True
    _last_heartbeat_sent = None

    _worker_threads = None

    _db_session_maker = None

    _task_manager = None


    def __init__(self, client_id, mqtt_broker_host=None, mqtt_server_port=1883, keep_alive=60):

        self._client_id = client_id
        self._topic_callbacks = dict()
        self._descriptor_callbacks = dict()

        threading.Thread.__init__(self)

        # P First thing, get a logger
        self._logger = logging.getLogger("ConsignmentShopMQTTThreadedClients")
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d %(levelname)-8s %(thread)d %(threadName)s @%(client_id)s %(message)s')
        if self._logger.handlers is None or len(self._logger.handlers) == 0:
            self._logger.addHandler(logging.StreamHandler(sys.stdout))
        for handler in self._logger.handlers:
            handler.setFormatter(formatter)
        self._logger = logging.LoggerAdapter(self._logger, {"client_id": self.client_id})
        self._logger.setLevel(logging.INFO)

        # P set up the Paho mqtt client to use
        self._mqtt_client = mqtt.Client(client_id=self.client_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message

        # P if a host was passed, do connect
        if mqtt_broker_host is not None:
            self.connect(mqtt_broker_host, mqtt_server_port, keep_alive)

    def add_remote_syslog_handler(self, syslog_server, syslog_port=514):
        remote_syslog_formatter = logging.Formatter(
            '@%(client_id)s ConsignmentShopMQTTThreadedClient %(asctime)s.%(msecs)03d %(levelname)-8s %(thread)d %(threadName)s @%(client_id)s %(message)s')
        remote_syslog_handler = logging.handlers.SysLogHandler(address=(syslog_server, syslog_port))
        remote_syslog_handler.setFormatter(remote_syslog_formatter)
        self.logger.logger.addHandler(remote_syslog_handler)

    def connect(self, mqtt_broker_host, mqtt_broker_port, keep_alive=60):
        # The Paho client keeps the host and port you pass it as private attributes. Keep my own copy for reference
        self._logger.debug("connecting {broker_host}:{broker_port}".format(broker_host=mqtt_broker_host,
                                                                           broker_port=mqtt_broker_port))
        self._mqtt_broker_host = mqtt_broker_host
        self._mqtt_broker_port = mqtt_broker_port
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    def on_connect(self, client, user_data, flags, rc):

        self.logger.debug("on_connect client='{client} user_data='{user_data}' flags='{flags}' rc='{rc}'".format(
            client=client, user_data=user_data, flags=flags, rc=rc))

        if rc == 0:
            if not self._mqtt_connected:
                self._logger.info("Connected to MQTT broker")
            self._mqtt_connected = True
            return

        # P anything be a rc 0 means the connection is messed up
        self._mqtt_connected = False
        if rc == 1:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 1, invalid protocol version)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 2:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 2, invalid client id)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 3:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 3, service unavailable)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 4:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 4, failed authorization)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 5:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 4, not authorized)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        else:
            self._logger.warning(
                "MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code {rc}, unknown response code)".format(
                    broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port, rc=rc))

    @mqtt_threaded_client_exception_catcher
    def on_message(self, client, user_data, msg):

        self._logger.debug("\\RCV\\ [client: {client}] [user_data: {user_data}] [topic:{topic}] \'{payload}\'".format(
            topic=msg.topic, payload=msg.payload, client=client, user_data=user_data))
        called_back_count = 0

        for topic_regex, callback in self._topic_callbacks.items():
            if re.match(topic_regex, msg.topic):
                callback(msg, self, self._logger)
                called_back_count += 1
            # QUESTION this will callback on multiple matches. Is that what I want?

        if called_back_count < 1:
            self._logger.info("No callback found for message on topic \'{topic}\'. Message unprocessed".
                              format(topic=msg.topic))

    def publish_on_topic(self, topic, payload):
        if not self._mqtt_connected:
            self._logger.warning(
                "Can't publish message, mqtt_client not connected [topic:{topic}] \'{payload}\'".format(topic=topic,
                                                                                                        payload=payload))
            return False

        self._logger.debug("\\PUB\\ [topic:{topic}] \'{payload}\'".format(topic=topic, payload=payload))
        return self._mqtt_client.publish(topic, payload)

    def subscribe_to_topic_with_callback(self, topic, callback):
        self.subscribe_to_topic(topic)
        self.per_topic_on_message_callback(topic, callback)

    def subscribe_to_topic(self, topic):

        # Topic must conform to spec http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718109
        self._logger.debug("\\SUB\\ [topic:{topic}]".format(topic=topic))

        # line contains non-utf8 character
        self._mqtt_client.subscribe(topic)

    def per_topic_on_message_callback(self, topic_regex, callback):
        compiled_regex = re.compile(topic_regex)
        self._logger.debug("Registering call back to {cb} for topic \'{topic_regex}\'".format(cb=callback,
                                                                                              topic_regex=topic_regex))
        self._topic_callbacks[compiled_regex] = callback

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._logger.info("Stopping listen loop and disconnecting from MQTT broker")
        self.stop_sending_heartbeats()
        self._mqtt_client.disconnect()

    def register_client_descriptor(self, descriptor_name, descriptor_callback):
        self._descriptor_callbacks[descriptor_name] = descriptor_callback

    def get_description(self):
        description = {}
        for descriptor, descriptor_callback in self._descriptor_callbacks.items():
            description[descriptor] = descriptor_callback(self)
        return description

    def assess_offer_needs(self, requirements):
        # TODO Implement offer assessment
        return True

    def send_heartbeat(self):
        hb_msg = {
            "client_id": self.client_id,
            "description": self.get_description()
        }
        if self.publish_on_topic(ConsignmentShop.topic_heartbeats, json.dumps(hb_msg)):
            self._last_heartbeat_sent = datetime.datetime.utcnow()

    def start_sending_heartbeats(self, interval_in_seconds=15):
        self._keep_sending_heart_beats = True
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeats_loop,
                                                  args=(self, interval_in_seconds))
        self._heartbeat_thread.start()

    def stop_sending_heartbeats(self):
        self._keep_sending_heart_beats = False
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(5)
            if self._heartbeat_thread.is_alive():
                self._logger.warning("Heartbeat thread did not die when stop_sending_heartbeats called")
            return not self._heartbeat_thread.is_alive()
        else:
            return True

    @staticmethod
    def _send_heartbeats_loop(self, interval_in_seconds):
        while self._keep_sending_heart_beats:
            self.send_heartbeat()
            time.sleep(interval_in_seconds)
        pass

    @property
    def logger(self):
        return self._logger

    @property
    def client_id(self):
        return self._client_id

    @property
    def db_session_maker(self):
        return self._db_session_maker

    @db_session_maker.setter
    def db_session_maker(self, db_session_maker):
        self._db_session_maker = db_session_maker

    @property
    def task_manager(self):
        return self._task_manager

    @task_manager.setter
    def task_manager(self, task_manager):
        self._task_manager = task_manager


class ConsignmentTask:

    @staticmethod
    def on_consignment_open(shop_client: ConsignmentShopMQTTThreadedClient, consignment: Consignment):
        pass

    @staticmethod
    def on_new_worker(shop_client: ConsignmentShopMQTTThreadedClient, worker_shadow: ConsignmentWorkerShadow):
        pass

    @staticmethod
    def on_result(shop_client: ConsignmentShopMQTTThreadedClient, result: ConsignmentResult):
        pass

    @staticmethod
    def on_worker_finished(shop_client: ConsignmentShopMQTTThreadedClient, worker_shadow: ConsignmentWorkerShadow):
        pass

    @staticmethod
    def on_consignment_completed(shop_client: ConsignmentShopMQTTThreadedClient, consignment: Consignment):
        pass

    @staticmethod
    def task(shop_client, task_parameters):
        pass


characteristics = {}


# TODO there needs to be a thread running for the keeper to periodically reevaluate the open consignments. If the consignment isn't optimal (healthy) need to make more offers
class ConsignmentCharacteristicException(Exception):
    pass


def characteristic(**kwargs):
    if 'name' not in kwargs.keys():
        raise ConsignmentCharacteristicException("Name is a required parameter of boolean_characteristic decorator")

    def wrapper_boolean_characteristic(func):
        characteristics[kwargs['name']] = {"f": func, "params": kwargs['params']}

        def wrapper(*args, **wrapper_kwargs):
            return func(*args, **wrapper_kwargs)

        return wrapper

    return wrapper_boolean_characteristic


def eval_consignment_characteristic(pattern, consignment_id, db_session_maker, logger):
    # Pull all directives out of a string in the format @DIRECTIVE_NAME(optional,list,of,parameters)
    find_all = "@(" + '|'.join(map(lambda k: k + "[()0-9,]*", characteristics.keys())) + ")"

    # split directive string in the format @DIRECTIVE_NAME(optional,list,of,parameters) info name and parameter regex groups
    split_name_from_parameters = r'(?P<name>[0-9a-zA-Z_]*)(?P<params>\(.*\))?'

    eval_pattern = pattern

    # TODO pre-compile these regex for speed
    directives = re.findall(find_all, pattern)
    for directive in directives:
        details = re.match(split_name_from_parameters, directive)
        if details.group('name') not in characteristics.keys():
            raise ConsignmentCharacteristicException("Unknown characteristic {}".format(details.group('name')))

        if details.group('params') is not None:
            # P Turn a string like "(10, foo, crap)" into a list of string values ['10', 'foo', 'crap']
            params = list(map(lambda p: p.strip(), re.sub(r'[()]', "", details.group('params')).split(",")))
            directive_result = characteristics[details.group('name')]['f'](consignment_id, db_session_maker, *params)
        else:
            directive_result = characteristics[details.group('name')]['f'](consignment_id, db_session_maker)

        eval_pattern = eval_pattern.replace("@{d}".format(d=str(directive)), str(directive_result), 1)

    logger.debug("for consignment {id} '{pattern}' => '{eval_pattern}'".format(id=consignment_id, pattern=pattern,
                                                                               eval_pattern=eval_pattern))
    return eval(eval_pattern)


@characteristic(name="TTL_EXPIRED", params=[])
def expired_ttl(consignment_id, session_maker):
    db_session = session_maker()
    consignment = db_session.query(Consignment).filter(Consignment.id == consignment_id).one_or_none()
    return (datetime.datetime.utcnow() - consignment.created_timestamp_utc).seconds > consignment.ttl_in_seconds


@characteristic(name="ACTIVE_WORKERS", params=["timout"])
def active_workers(consignment_id, session_maker, timeout):
    timeout = int(timeout)
    oldest = datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout)
    db_session = session_maker()
    workers_active = db_session.query(ConsignmentResult).filter(
        sqlalchemy.and_(ConsignmentResult.consignment_id == consignment_id,
                        ConsignmentResult.created_timestamp_utc >= oldest)).order_by(
        ConsignmentResult.created_timestamp_utc).group_by(ConsignmentResult.worker_shadow_id).count()

    db_session.close()
    return workers_active


@characteristic(name="OPEN_CONTRACTS", params=[])
def contracts_open(consignment_id, session_maker):
    db_session = session_maker()
    open_contracts = db_session.query(ConsignmentContract).filter(
        sqlalchemy.and_(ConsignmentContract.consignment_id == consignment_id,
                        ConsignmentContract.state == ConsignmentContract.STATE_OPEN)).count()
    db_session.close()
    return open_contracts


@characteristic(name="RECENTLY_OPENED_CONTRACTS", params=[])
def contracts_recently_opened(consignment_id, session_maker, timeout=5):
    timeout = int(timeout)
    oldest = datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout)

    db_session = session_maker()
    open_contracts = db_session.query(ConsignmentContract).filter(
        sqlalchemy.and_(ConsignmentContract.consignment_id == consignment_id,
                        ConsignmentContract.state == ConsignmentContract.STATE_OPEN,
                        ConsignmentContract.created_timestamp_utc >= oldest)).count()
    db_session.close()
    return open_contracts


@characteristic(name="FINISHED_WORKERS", params=[])
def finished_workers(consignment_id, session_maker):
    db_session = session_maker()
    workers_finished = db_session.query(ConsignmentResult).filter(
        sqlalchemy.and_(ConsignmentResult.consignment_id == consignment_id,
                        ConsignmentResult.worker_finished is True)).group_by(ConsignmentResult.worker_shadow_id).count()
    return workers_finished


@characteristic(name="TOTAL_RESULTS", params=[])
def total_results(consignment_id, session_maker):
    db_session = session_maker()
    result_total = db_session.query(ConsignmentResult).filter(
        sqlalchemy.and_(ConsignmentResult.consignment_id == consignment_id,
                        ConsignmentResult.results is not None)).count()
    return result_total
