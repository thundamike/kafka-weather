from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
from report_pb2 import Report

def send_temps():
    broker = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers=[broker], acks='all', retries=10)
    for date, degrees in weather.get_next_weather(delay_sec=0.1):
        msg = Report(date=date, degrees=degrees).SerializeToString()
        months = {'01': 'January', '02': 'February','03': 'March','04': 'April','05': 'May', '06': 'June','07': 'July',
          '08': 'August','09': 'September','10': 'October','11': 'November','12': 'December'
        }
        month = date.split('-')[1]
        month = months[month]
        producer.send("temperatures", value=msg, key=bytes(month, "utf-8"))

def main():
    broker = 'localhost:9092'
    admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    try:
        admin_client.delete_topics(["temperatures"])
        print("Deleted topics successfully")
    except UnknownTopicOrPartitionError:
        print("Cannot delete topic/s (may not exist yet)")
    
    #time.sleep(3) # Deletion sometimes takes a while to reflect - COMMENTING OUT FOR DEBUG
    
    # Create topic 'temperatures' with 4 partitions and replication factor = 1
    admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])

    # send reports to the temperatures topic
    send_temps()

if __name__ == "__main__":
    main()

