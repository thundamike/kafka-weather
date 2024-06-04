import os
import sys
import json
from kafka import TopicPartition, KafkaConsumer
from report_pb2 import Report
from datetime import datetime
import threading

def atomic_write(data, path):
    path2 = path + ".tmp"
    with open(path2, "w") as f:
        json.dump(data, f)
        os.rename(path2, path)

def temp_consumer(partition):
    broker = "localhost:9092"
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    consumer.assign([TopicPartition("temperatures", partition)])
    
    ## check if file created
    file = f"partition-{partition}.json"
    file_path = os.path.join('/files/', file)
    if os.path.exists(file_path):
        pass
    else:
        init = {"partition": partition, "offset": 0}
        with open(file_path, 'w') as file:
            json.dump(init, file)

    ## seek to offset specified by json file
    data = ''
    with open(file_path, 'r') as file:
        data = json.load(file)
        consumer.seek(TopicPartition("temperatures", partition), data['offset'])

    ## infinite loop to poll for new messages
    while True:
        batch = consumer.poll(1000)
        prev = ''
        for tp, messages in batch.items():
            for msg in messages:
                r = Report.FromString(msg.value)
                date = datetime.strptime(r.date, '%Y-%m-%d')
                if prev != '' and date <= prev:
                    # skip over current iteration if a duplicate or earlier date is read
                    continue
                    
                month = str(msg.key, "utf-8")
                if month not in data:
                    data[month] = {}
                year = r.date.split('-')[0]
                if year not in data[month]:
                    data[month][year] = {}
                deg = r.degrees

                if 'end' not in data[month][year] or 'start' not in data[month][year]:
                    data[month][year]['end'] = r.date
                    data[month][year]['start'] = r.date
                else:
                    curr_end = datetime.strptime(data[month][year]['end'], '%Y-%m-%d')
                    curr_start = datetime.strptime(data[month][year]['start'], '%Y-%m-%d')
                    ## duplicate condition check
                    if date <= curr_end:
                        continue
                    if date > curr_end:
                        data[month][year]['end'] = r.date
                    if date < curr_start:
                        data[month][year]['start'] = r.date
                
                if 'count' not in data[month][year]:
                    data[month][year]['count'] = 1
                else:
                    data[month][year]['count'] += 1
                if 'sum' not in data[month][year]: 
                    data[month][year]['sum'] = deg
                else:
                    data[month][year]['sum'] += deg      
                data[month][year]['avg'] = data[month][year]['sum'] / data[month][year]['count']

                pos = consumer.position(TopicPartition("temperatures", partition))  ## gets current offset
                data['offset'] = pos
                prev = date
                atomic_write(data, file_path)
            

def main():
    partitions = [int(part) for part in sys.argv[1:]]
    threads = []
    for p in partitions:
        print(p)
        t = threading.Thread(target=temp_consumer, args=(p,))
        threads.append(t)
        t.start()
    for thread in threads:
        thread.join()
    
if __name__ == "__main__":
    main()