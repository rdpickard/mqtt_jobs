import logging
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
    return "{prefix}{rando}".format(prefix=prefix, rando=secrets.token_hex(length))


def python_fstring_to_regex(fstring):
    return re.sub("{([a-zA-Z0-9_]*)}", "(?P<\\1>.[a-zA-Z0-9]*)", fstring)


class ConsignmentClientException(Exception):
    pass


msg_json_schema_offer_response = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Consignment Offer Response Message",
    "type": "object",
    "required": ["offer_id", "client_id"],
    "properties": {
        "offer_id": {
            "type": "string",
            "description": "The id of the offer"
        },
        "client_id": {
            "type": "string",
            "description": "Client that want the contract for the offer"
        }
    }

}

msg_json_schema_offer = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Consignment Offer Message",
    "type": "object",
    "required": ["offer_id", "task_name", "worker_parameters"],
    "properties": {
        "offer_id": {
            "type": "string",
            "description": "The id of the offer"
        },
        "task_name": {
            "type": "string",
            "description": "The task the offer need completed"
        },
        "worker_parameters": {
            "type": "object",
            "description": "The qualities a worker needs to complete the task"
        }
    }
}

msg_json_schema_heartbeat = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "HeartBeat Message",
    "type": "object",
    "required": ["client_id", "description"],
    "properties": {
        "client_id": {
            "type": "string",
            "description": "The id of the client"
        },
        "description": {
            "type": "object",
            "description": "Dictionary of descriptors keyed off of descriptor name"
        }
    }
}

msg_json_schema_task_results = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Results Message",
    "type": "object",
    "required": ["client_id", "offer_id", "results", "results_encoding", "work_sequence", "finished", "sent_timestamp_utc"],
    "properties": {
        "client_id": {
            "type": "string",
            "description": "The id of the client"
        },
        "offer_id": {
            "type": "string",
            "description": "The id of the consignment offer the client is working off from"
        },
        "results": {
            "type": "string",
            "description": "The results of the task"
        },
        "results_encoding": {
            "type": "string",
            "description": "The encoding of the result value"
        },
        "work_sequence": {
            "type": "integer",
            "description": "The place in the order of all results this message represents"
        },
        "finished": {
            "type": "boolean",
            "description": "The worker is done."
        },
        "sent_timestamp_utc": {
            "type": "string",
            "description": "The time the result was sent from the worker"
        }
    }
}


class ConsignmentShop:

    _name = None

    topic_offers_dispatch = "mqtt/workers/offers/dispatch"
    topic_worker_contracts = "mqtt/workers/{client_id}/contracts"
    topic_keeper_accepted_offer = "mqtt/keeper/offers/inbox"
    topic_consignment_results = "mqtt/keeper/consignment/{consignment_id}/results"
    topic_heartbeats = "mqtt/keeper/heartbeats"

    @staticmethod
    def new_contract(msg, shop_client, _):
        payload = json.loads(msg.payload)
        shop_client.logger.critical("I GOT THE JOB!")

        ConsignmentResult.publish_result(shop_client,
                                         "{}",
                                         payload["offer_id"],
                                         payload["consignment_id"],
                                         0, False)

    @property
    def name(self):
        return self._name

    @staticmethod
    def consignment_worker_factory(tasks, client_id, mqtt_broker_host, mqtt_broker_port=1883):
        client = ConsignmentShopMQTTThreadedClient(client_id, mqtt_broker_host, mqtt_broker_port)

        # Listen for new offers
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_offers_dispatch,
                                                ConsignmentOffer.incoming_offer)

        # Listen for contracts to offers I bid for
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_worker_contracts.format(client_id=client_id),
                                                ConsignmentShop.new_contract)

        client.send_heartbeat()
        client.start()

        client.start_sending_heartbeats(1)

        return client

    @staticmethod
    def consignment_keeper_factory(tasks, db_uri, client_id, mqtt_broker_host, mqtt_broker_port=1883, db_echo=False):
        client = ConsignmentShopMQTTThreadedClient(client_id, mqtt_broker_host, mqtt_broker_port)

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
        client.subscribe_to_topic_with_callback(ConsignmentShop.topic_consignment_results.format(consignment_id="([a-zA-Z0-9]*)"),
                                                ConsignmentResult.incoming_result)

        client.start()

        return client


class Consignment(Base):

    __tablename__ = "consignment"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    description = sqlalchemy.Column(sqlalchemy.String)
    descriptive_details = sqlalchemy.Column(sqlalchemy.PickleType)
    # TODO add descriptive tags to Consignment

    task_name = sqlalchemy.Column(sqlalchemy.String)
    task_details = sqlalchemy.Column(sqlalchemy.PickleType)
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

    results = sqlalchemy.orm.relationship("ConsignmentResult")
    offers = sqlalchemy.orm.relationship("ConsignmentOffer")

    @staticmethod
    def new_consignment(shop_client, name: str, task_name: str, task_details: dict, worker_parameters: dict,
                        description: str = None, descriptive_details: dict = None):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new Consignment client has no db session maker")

        consignment = Consignment()
        consignment.id = gen_hex_id("C")
        consignment.name = name
        consignment.task_name = task_name
        consignment.task_details = task_details
        consignment.worker_parameters = worker_parameters
        consignment.description = description
        consignment.descriptive_details = descriptive_details

        db_session = shop_client.db_session_maker()
        db_session.add(consignment)
        db_session.flush()
        db_session.commit()

        shop_client.subscribe_to_topic(ConsignmentShop.topic_consignment_results.format(consignment_id=consignment.id))

        return consignment

    def make_new_offer(self, shop_client, offer_description="opportunity awaits from the top!"):

        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")
        if self.state == self.STATE_COMPLETED or self.state == self.STATE_ABANDONED:
            raise ConsignmentClientException("Can't make new offer consignment '{id}' was closed at {closed} in state '{state}'".format(
                id=self.id, closed=self.closed_timestamp_utc, state=self.state
            ))

        offer = ConsignmentOffer.new_offer_for_consignment(shop_client, self, offer_description)

        db_session = shop_client.db_session_maker()
        if self.state == self.STATE_NEW:
            self.state = self.STATE_ACTIVE
        self.last_updated_timestamp_utc = datetime.datetime.utcnow()
        db_session.flush()
        db_session.commit()

        return offer

    def send_more_contracts(self):
        # TODO write code to evaluate if consignment needs more workers
        return True


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

    @staticmethod
    def new_offer_for_consignment(shop_client, consignment, description="opportunity awaits", open_and_publish=True):
        if shop_client.db_session_maker is None:
            raise ConsignmentClientException("Can't make new offer client has no db session maker")
        if consignment.state == Consignment.STATE_COMPLETED or consignment.state == Consignment.STATE_ABANDONED:
            shop_client.logger.warning("Creating an offer for consignment '{id}' which is finished".format(id=consignment.id))

        db_session = shop_client.db_session_maker()

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
            shop_client.publish_on_topic(ConsignmentShop.topic_offers_dispatch, ConsignmentOffer.dumps_offer_message(offer))

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
                                                                                         payload=str(offer_msg.payload)))
            return

        if shop_client.assess_offer_needs(payload['worker_parameters']):
            shop_client.publish_on_topic(ConsignmentShop.topic_keeper_accepted_offer,
                                         ConsignmentOffer.dumps_offer_response_message(payload['offer_id'], shop_client))

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
            logger.warning("Offer resonse message payload failed validation. Ignoring message.")
            logger.debug("Ignored message validation err '{err}'".format(err=ve))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic,
                                                                                         payload=str(offer_response_msg.payload)))
            return

        db_session = shop_client.db_session_maker()

        # P Check that the offer exists and is in a state to consider new contract
        offer = db_session.query(ConsignmentOffer).filter_by(id=payload['offer_id']).one_or_none()
        if offer is None:
            logger.warning("No consignment offer with id \'{id}\' in DB. Ignoring message.".format(
                id=payload["offer_id"]))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic, payload=json.dumps(payload)))
            return
        if offer.state != ConsignmentOffer.STATE_OPEN:
            logger.warning("Offer \'{id}\' is not '{open}' is '{state}'. Ignoring message.".format(
                id=offer.id, open=ConsignmentOffer.STATE_OPEN, state=offer.state))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=offer_response_msg.topic, payload=json.dumps(payload)))
            return

        if offer.consignment.send_more_contracts():
            # P The consignment wants more workers, send the worker a contract
            contract = ConsignmentContract.new_contract_for_client_for_consignment(shop_client, offer.id, payload['client_id'])
            if contract is None:
                logger.error("Could not create a contract with client '{cid}' for offer '{id}'".format(cid=payload['client_id'], id=offer.id))
                return
            shop_client.publish_on_topic(ConsignmentShop.topic_worker_contracts.format(client_id=payload['client_id']),
                                         contract.dumps_contract_message())


class ConsignmentContract(Base):
    __tablename__ = "consignment_contract"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    offer_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_offer.id'))
    offer = sqlalchemy.orm.relationship("ConsignmentOffer", back_populates="contracts")
    worker_shadow_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment_worker_shadow.id'))

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow())
    sent_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    closed_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    def dumps_contract_message(self):
        msg = {
            "consignment_id": self.offer.consignment.id,
            "offer_id": self.offer.id,
            "contract_id": self.id,
        }
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

        contract = ConsignmentContract()
        contract.id = gen_hex_id("XT")
        contract.offer_id = offer.id
        contract.offer = offer
        contract.worker_shadow_id = shadow.id

        db_session.flush()
        db_session.commit()

        shop_client.logger.critical("cons "+str(contract.offer_id))

        return contract


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
                                                                                         payload=str(result_msg.payload)))
            return
        except jsonschema.ValidationError as ve:
            logger.warning("Results message payload failed validation. Ignoring message.")
            logger.debug("Ignored message validation err '{err}'".format(err=ve))
            logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic,
                                                                                         payload=str(result_msg.payload)))
            return

        db_session = shop_client.db_session_maker()
        try:

            # P Get the offer that the worker signed up to
            offer = db_session.query(ConsignmentOffer).filter_by(id=payload['offer_id']).one_or_none()
            if offer is None:
                logger.warning("No consignment offer with id \'{id}\' in DB. Ignoring message.".format(
                    id=payload["offer_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
                return
            if offer.state != ConsignmentOffer.STATE_OPEN:
                logger.warning("Offer \'{id}\' is not '{open}' is '{state}'. Ignoring message.".format(
                    id=offer.id, open=ConsignmentOffer.STATE_OPEN, state=offer.state))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))

            # MAYBE make sure the worker has a valid contract to do this work

            # P Make sure the consignment exists, is still open and the topic, and offer match

            consignment_id_match = re.match(python_fstring_to_regex(ConsignmentShop.topic_consignment_results), result_msg.topic)
            if consignment_id_match is None or consignment_id_match.group('consignment_id') is None:
                logger.warning("Could not find consignment id in msg topic {topic} Ignoring message.".format(
                    topic=result_msg.topic))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
                return
            consignment_id = consignment_id_match.group('consignment_id')
            if consignment_id != payload['consignment_id']:
                logger.warning("Topic '{tid}' and message consignment id '{mid}' don't match Ignoring message.".format(
                    tid=consignment_id, mid=payload['consignment_id']))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
                return

            consignment = db_session.query(Consignment).filter_by(id=payload['consignment_id']).one_or_none()
            if consignment is None:
                logger.warning("No consignment  with id \'{id}\' in DB. Ignoring message.".format(
                    id=payload["consignment_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
                return
            if offer.consignment_id != consignment.id:
                logger.warning("Offer \'{oid}\' has different consignment id of '{ocid}' than the consignment id of the topic {tcid}. Ignoring message".format(
                    oid=offer.id, ocid=offer.consignment_id, tcid=consignment.id))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
                return

            # P get the shadow of the worker that is reporting the results
            shadow = db_session.query(ConsignmentWorkerShadow).filter_by(client_id=payload['client_id']).one_or_none()
            if shadow is None:
                logger.warning("No worker shadow for with client id \'{id}\' in DB. Ignoring message.".format(id=payload["client_id"]))
                logger.debug("Ignored message payload [topic: {topic}] \'{payload}\'".format(topic=result_msg.topic, payload=json.dumps(payload)))
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

            db_session.flush()
            db_session.commit()

            # TODO Check consignment on what should be done when a result is stored (task callbacks)

        except sqlalchemy.orm.exc.MultipleResultsFound:
            logger.warning("More than one worker shadow in DB with client id \'{id}\'".format(id=payload["client_id"]))
            db_session.rollback()

    @staticmethod
    def publish_result(shop_client, results: str, offer_id: str, consignment_id: str, work_sequence: int, finished=False):
        encoding_guess = ""

        result_msg = {
            "client_id": shop_client.client_id,
            "offer_id": offer_id,
            "consignment_id": consignment_id,
            "results": results,
            "results_encoding": encoding_guess,
            "work_sequence": work_sequence,
            "finished": finished,
            "sent_timestamp_utc": str(datetime.datetime.utcnow())
        }

        shop_client.publish_on_topic(ConsignmentShop.topic_consignment_results.format(consignment_id=consignment_id),
                                     json.dumps(result_msg))


class ConsignmentTask:
    pass


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

        logger.debug("💖 "+payload['client_id'])

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

    def mqtt_threaded_client_exception_catcher(func):

        def wrapper(*args):
            try:
                return func(*args)
            except Exception as e:
                tb = traceback.format_exc()
                logger = args[0]._logger
                logger.error("Uncaught exception {function} \"{error}\" \"{tb}\"".format(
                                                                                  function=str(func),
                                                                                  error=str(e),
                                                                                  tb=str(
                                                                                      base64.b64encode(
                                                                                          tb.encode("utf-8")),
                                                                                      "utf-8")))
                logger.debug(tb)

        return wrapper

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
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(formatter)
        self._logger = logging.LoggerAdapter(self._logger, {"client_id": self.client_id})
        self._logger.setLevel(logging.DEBUG)

        # P set up the Paho mqtt client to use
        self._mqtt_client = mqtt.Client(client_id=self.client_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message

        # P if a host was passed, do connect
        if mqtt_broker_host is not None:
            self.connect(mqtt_broker_host, mqtt_server_port, keep_alive)

    def connect(self, mqtt_broker_host, mqtt_broker_port, keep_alive=60):
        # The Paho client keeps the host and port you pass it as private attributes. Keep my own copy for reference
        self._logger.debug("connecting {broker_host}:{broker_port}".format(broker_host=mqtt_broker_host,
                                                                           broker_port=mqtt_broker_port))
        self._mqtt_broker_host = mqtt_broker_host
        self._mqtt_broker_port = mqtt_broker_port
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            if not self._mqtt_connected:
                self._logger.info("Connected to MQTT broker")
            self._mqtt_connected = True
            return

        # P anything be a rc 0 means the connection is messed up
        self._mqtt_connected = False
        if rc == 1:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 1, invalid protocol version)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 2:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 2, invalid client id)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 3:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 3, service unavailable)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 4:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 4, failed authorization)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        elif rc == 5:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code 4, not authorized)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port))
        else:
            self._logger.warning("MQTT broker \'{broker_host}:{broker_port}\'refused connection (return code {rc}, unknown response code)".format(broker_host=self._mqtt_broker_host, broker_port=self._mqtt_broker_port, rc=rc))

    @mqtt_threaded_client_exception_catcher
    def on_message(self, client, userdata, msg):

        self._logger.debug("\\RCV\\ [topic:{topic}] \'{payload}\'".format(topic=msg.topic, payload=msg.payload))
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
            self._logger.warning("Can't publish message, mqtt_client not connected [topic:{topic}] \'{payload}\'".format(topic=topic, payload=payload))
            return False

        self._logger.debug("\\PUB\\ [topic:{topic}] \'{payload}\'".format(topic=topic, payload=payload))
        return self._mqtt_client.publish(topic, payload)

    def subscribe_to_topic_with_callback(self, topic, callback):
        self.per_topic_on_message_callback(topic, callback)
        self.subscribe_to_topic(topic)

    def subscribe_to_topic(self, topic):
        self._logger.debug("\\SUB\\ [topic:{topic}]".format(topic=topic))
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
