import pandas as pd
import matplotlib.pyplot as plt
import json
import os

def main():

    # Read from JSONs and find the average max-temperature for the latest recorded year of jan, feb, mar
    dates = {}
    for i in range(4):
        file = f"partition-{i}.json"
        file_path = os.path.join('/files/', file)
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)
                if 'January' in data:
                    latest_year = max(data['January'].keys(), key=lambda x: int(x))
                    date = f'January-{latest_year}'
                    avg = data['January'][latest_year]['avg']
                    dates[date] = avg
        
                if 'February' in data:
                    latest_year = max(data['February'].keys(), key=lambda x: int(x))
                    date = f'February-{latest_year}'
                    avg = data['February'][latest_year]['avg']
                    dates[date] = avg
        
                if 'March' in data:
                    latest_year = max(data['March'].keys(), key=lambda x: int(x))
                    date = f'March-{latest_year}'
                    avg = data['March'][latest_year]['avg']
                    dates[date] = avg
           
        else:
            pass

    ## check if there is data for plotting
    if not dates or len(dates)==0:
        # default values that indicate error
        dates['January-1990'] = 0
        dates['February-1990'] = 0
        dates['March-1990'] = 0
        
    month_series = pd.Series(dates)
    fig, ax = plt.subplots()
    month_series.plot.bar(ax=ax)
    ax.set_ylabel('Avg. Max Temperature')
    plt.tight_layout()
    plt.savefig("/files/month.svg")

if __name__ == "__main__":
    main()