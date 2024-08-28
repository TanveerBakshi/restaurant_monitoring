import pandas as pd
import uuid
import threading
from flask import Flask, jsonify, request, send_file
from datetime import datetime, timedelta
import os

app = Flask(__name__)


REPORTS = {}

#Hardcoded max timestamp 
MAX_TIMESTAMP = datetime.fromisoformat('2023-01-25T18:13:22.479220+00:00')

def calculate_uptime_downtime(store_id, status_logs):
    store_data = status_logs[status_logs['store_id'] == store_id]
    uptime_last_hour = uptime_last_day = uptime_last_week = 0
    downtime_last_hour = downtime_last_day = downtime_last_week = 0
    
    #Define time intervals
    last_hour_start = MAX_TIMESTAMP - timedelta(hours=1)
    last_day_start = MAX_TIMESTAMP - timedelta(days=1)
    last_week_start = MAX_TIMESTAMP - timedelta(weeks=1)

    for start_time, end_time, granularity in [
        (last_hour_start, MAX_TIMESTAMP, 'hour'),
        (last_day_start, MAX_TIMESTAMP, 'day'),
        (last_week_start, MAX_TIMESTAMP, 'week')
    ]:
        interval_data = store_data[(store_data['timestamp_utc'] >= start_time) & (store_data['timestamp_utc'] <= end_time)]
        
        last_status = None
        last_timestamp = None
        
        for _, row in interval_data.iterrows():
            status = row['status']
            current_timestamp = row['timestamp_utc']

            if last_status is not None and last_timestamp is not None:
                duration = current_timestamp - last_timestamp
                duration_minutes = duration.total_seconds() / 60

                if last_status == 'active':
                    if granularity == 'hour':
                        uptime_last_hour += duration_minutes
                    elif granularity == 'day':
                        uptime_last_day += duration_minutes / 60
                    elif granularity == 'week':
                        uptime_last_week += duration_minutes / 60
                else:
                    if granularity == 'hour':
                        downtime_last_hour += duration_minutes
                    elif granularity == 'day':
                        downtime_last_day += duration_minutes / 60
                    elif granularity == 'week':
                        downtime_last_week += duration_minutes / 60

            last_status = status
            last_timestamp = current_timestamp
        
        if last_status == 'active' and last_timestamp is not None:
            end_time = MAX_TIMESTAMP
            duration = end_time - last_timestamp
            duration_minutes = duration.total_seconds() / 60

            if granularity == 'hour':
                uptime_last_hour += duration_minutes
            elif granularity == 'day':
                uptime_last_day += duration_minutes / 60
            elif granularity == 'week':
                uptime_last_week += duration_minutes / 60
        elif last_status == 'inactive' and last_timestamp is not None:
            end_time = MAX_TIMESTAMP
            duration = end_time - last_timestamp
            duration_minutes = duration.total_seconds() / 60

            if granularity == 'hour':
                downtime_last_hour += duration_minutes
            elif granularity == 'day':
                downtime_last_day += duration_minutes / 60
            elif granularity == 'week':
                downtime_last_week += duration_minutes / 60

    return {
        'store_id': store_id,
        'uptime_last_hour': uptime_last_hour,
        'uptime_last_day': uptime_last_day,
        'uptime_last_week': uptime_last_week,
        'downtime_last_hour': downtime_last_hour,
        'downtime_last_day': downtime_last_day,
        'downtime_last_week': downtime_last_week
    }

def generate_report():
   
    store_status_file = 'store status.csv'  
    store_hours_file = 'Menu hours.csv'  
    store_timezones_file = 'bq-results-20230125-202210-1674678181880.csv'  
    try:
        status_logs = pd.read_csv(store_status_file)
        business_hours = pd.read_csv(store_hours_file)
        timezones = pd.read_csv(store_timezones_file)
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        return []

    #Convert timestamps
    try:
        status_logs['timestamp_utc'] = pd.to_datetime(status_logs['timestamp_utc'], errors='coerce')
    except Exception as e:
        print(f"Error converting timestamps: {e}")
        return []

    #Calculate uptime and downtime for each store
    report_data = []
    for store_id in status_logs['store_id'].unique():
        report_data.append(calculate_uptime_downtime(store_id, status_logs))

    report_file = f'report_{uuid.uuid4()}.csv'
    try:
        pd.DataFrame(report_data).to_csv(report_file, index=False)
    except Exception as e:
        print(f"Error saving report to CSV: {e}")
        return []

    return report_file

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    report_id = str(uuid.uuid4())
    REPORTS[report_id] = {'status': 'Running', 'file': None}
    def generate_report_thread():
        try:
            report_file = generate_report()
            REPORTS[report_id]['status'] = 'Complete'
            REPORTS[report_id]['file'] = report_file
        except Exception as e:
            REPORTS[report_id]['status'] = 'Error'
            print(f"Error generating report: {e}")
    
    thread = threading.Thread(target=generate_report_thread)
    thread.start()
    
    return jsonify({'report_id': report_id})

@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    if report_id in REPORTS:
        report_info = REPORTS[report_id]
        if report_info['status'] == 'Complete':
            if report_info['file'] and os.path.exists(report_info['file']):
                return send_file(report_info['file'], mimetype='text/csv', as_attachment=True)
            else:
                return jsonify({'status': 'Report file not found'}), 404
        else:
            return jsonify({'status': 'Running'}), 200
    else:
        return jsonify({'status': 'Invalid report_id'}), 400

if __name__ == '__main__':
    app.run(debug=True)







