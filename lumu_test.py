import json
import requests
import sys

from collections import defaultdict
from datetime import datetime

URL = "https://api.lumu.io/collectors/{collector_id}/dns/queries?key={lumu_client_key}"
CHUNKS_SIZE = 500


def main():
    if len(sys.argv) != 4:
        sys.exit("Usage: python lumu_test.py [file_name] [client_key] [collector_id]")

    file_name = sys.argv[1]
    client_key = sys.argv[2]
    collector_id = sys.argv[3]

    # Read file
    with open (file_name, "r") as f:
        records = f.read().split("\n")

    json_records = []
    client_ips_queries_count = defaultdict(int)
    host_queried_count = defaultdict(int)

    # Convert strings into objects with only the required data
    for record in records[:-1]:
        record = record.split(" ")
        new_json_record = {
            "timestamp": datetime.strptime(f"{record[0]} {record[1]}", "%d-%b-%Y %H:%M:%S.%f").isoformat(),
            "name": record[9],
            "client_ip": record[6].split("#")[0],
            "client_name": record[5][3:],
            "type": record[11]
        }

        # Add record to list of reccords
        json_records.append(new_json_record)

        # Add +1 to counter of client ip queries for that specific client_ip
        client_ips_queries_count[new_json_record["client_ip"]] += 1
        # Add +1 to counter of queried hosts for that specific host
        host_queried_count[new_json_record["name"]] += 1


    # Send data to API in chunks
    for records_chunk in split_list_in_chunks(json_records, CHUNKS_SIZE + 200):
        requests.post(
            url=URL.format(collector_id=collector_id, lumu_client_key=client_key),
            data=json.dumps(records_chunk)
        )

    total_records = len(json_records)
    sorted_client_ips_queries_count = sorted(client_ips_queries_count.items(), key=lambda x: x[1], reverse=True)
    sorted_host_queried_count = sorted(host_queried_count.items(), key=lambda x: x[1], reverse=True)

    longest_ip_count = len(str(sorted_client_ips_queries_count[0][1]))
    longest_domain_count = len(str(sorted_host_queried_count[0][1]))

    max_ip_count_percentage_length = len(str(int((sorted_client_ips_queries_count[0][1] / total_records) * 100))) + 4
    max_domain_count_percentage_length = len(str(int((sorted_host_queried_count[0][1] / total_records) * 100))) + 4
    
    sys.stdout.write(f"\nTotal records {total_records}\n")
    sys.stdout.write(f"\n\n")
    sys.stdout.write("Client IPs Rank\n")
    sys.stdout.write("-"*15 + "  " + "-"*longest_ip_count + "  " + "-"*max_ip_count_percentage_length + "\n")

    for client_ip in sorted_client_ips_queries_count[0:5]:

        percentage = '{:0.2f}'.format((client_ip[1] / total_records) * 100) + "%"

        sys.stdout.write(f"{client_ip[0]}")
        sys.stdout.write(
            " "*(17-len(client_ip[0]) + (longest_ip_count - len(str(client_ip[1]))))
        )
        sys.stdout.write(f"{client_ip[1]}")
        sys.stdout.write(
            " "*(2 + max_ip_count_percentage_length - len(percentage))
        )
        sys.stdout.write(f"{percentage}\n")
        
    sys.stdout.write("-"*15 + "  " + "-"*longest_ip_count + "  " + "-"*max_ip_count_percentage_length + "\n")
    sys.stdout.write(f"\n\n")
    sys.stdout.write("Host Rank\n")
    sys.stdout.write("-"*60 + "  " + "-"*longest_domain_count + "  " + "-"*max_domain_count_percentage_length + "\n")
    
    for host in sorted_host_queried_count[0:5]:

        percentage = '{:0.2f}'.format((host[1] / total_records) * 100) + "%"

        if len(host[0]) > 55:
            sys.stdout.write(f"{host[0][:55]}...")

        else:
            sys.stdout.write(f"{host[0]}")

        sys.stdout.write(
            " "*(62-len(host[0]) + (longest_domain_count - len(str(host[1]))))
        )
        
        sys.stdout.write(f"{host[1]}")
        sys.stdout.write(
            " "*(2 + max_domain_count_percentage_length - len(percentage))
        )
        sys.stdout.write(f"{percentage}\n")

    sys.stdout.write("-"*60 + "  " + "-"*longest_domain_count + "  " + "-"*max_domain_count_percentage_length + "\n")


def split_list_in_chunks(list, chunk_size):

  for i in range(0, len(list), chunk_size):
    yield list[i:i + chunk_size]


if __name__ == "__main__":
    main()