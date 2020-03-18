from jobber_mqtt_details import *
import jobber_dispatcher
import time

if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

db_connection_uri = 'sqlite:////tmp/db.sqlite?check_same_thread=False'
db_engine = sqlalchemy.create_engine(db_connection_uri, echo=False)
db_session_maker = sqlalchemy.orm.sessionmaker(bind=db_engine)
db_session = db_session_maker()

dispatcher = jobber_dispatcher.JobberDispatcher(db_connection_uri,
                                                "dispatcher-thing", "localhost")

ten_workers = BehaviorAfterNFinishedWorkers(count=10)
five_seconds = BehaviorAfterNSecondsActivity(seconds=10)

consignment_id = dispatcher.new_consignment("count", "testing behavior pattern", {}, {}, None, None, str(five_seconds))
consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()

time.sleep(5)

print(five_seconds.test(consignment.job_pattern, consignment, db_session))
