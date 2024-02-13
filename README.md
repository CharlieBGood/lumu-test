To run the script execute:

python lumu_test.py [file_name] [client_key] [collector_id]

where:

file_name = Is the name of the BIND Server log file if located inside the same directory as lumu_test.py. Else use absolute path to the file.

client_key = lumu-client-key

collector_if = collector-id

Requirements: python 3.6+

The computational complexity of the ranking algorithm is O(n log(n)) since it relies on the built in python sorted function which uses Timsort algorithm.
