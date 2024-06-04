from kafka import KafkaConsumer
from report_pb2 import Report

def main():
    broker = "localhost:9092"
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    consumer.subscribe(["temperatures"])
    while True:
        batch = consumer.poll(1000)
        for tp, messages in batch.items():
            for msg in messages:
                r = Report.FromString(msg.value)
                d = {}
                d['partition'] = tp.partition
                d['key'] = str(msg.key, "utf-8")
                d['date'] = r.date
                d['degrees'] = r.degrees
                print(d)

if __name__ == "__main__":
    main()