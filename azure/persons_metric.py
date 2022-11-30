from datetime import datetime
import azure.req as req

def create_persons_metric_table(connection):
    SQL = """                   CREATE TABLE persons_metric (
                                    person_id BIGINT PRIMARY KEY NOT NULL,
                                    clicksToConvert INTEGER,
                                    clicksToShare INTEGER,
                                    timeToConvert INTEGER,
                                    timeToShare INTEGER,
                                    FOREIGN KEY (person_id) REFERENCES persons (person_id),
                                    UNIQUE(person_id)
                                )
        """
    cursor = connection.cursor()
    cursor.execute(SQL)
    connection.commit()

def insert_persons_metric_table(connection):
    query =                             """SELECT persons.person_id,
                                                events.clicked_date,
                                                events.event_name
                                        from events
                                        INNER JOIN persons
                                        ON persons.person_id = events.person_id
                                        ORDER BY events.clicked_date ASC"""
    cursor = connection.cursor()
    cursor.fast_executemany = True
    res = cursor.execute(query).fetchall()
    print("Starting to insert")
    person_id_dict = {}
    for event in res:
        person_id = event[0]
        time = event[1]
        event_name = event[2]

        if person_id not in person_id_dict:
            person_id_dict[person_id] = [[time, event_name]]
        else:
            person_id_dict[person_id].append([time, event_name])
    
    print("step1")
    # Computing all the metrics
    persons_list = []
    for person_id in person_id_dict.keys():
        events = person_id_dict[person_id]

        ctc = None
        ttc = None
        counter = 0
        start = events[0][0]
        for event in events:
            counter += 1
            if event[1] == 'conversion':
                ctc = counter
                ttc = round((event[0] - start).total_seconds())
                break

        cts = None
        tts = None
        counter = 0
        start = events[0][0]
        for event in events:
            counter += 1
            if event[1] == 'share_experience':
                cts = counter
                tts = round((event[0] - start).total_seconds())
                break
            
        persons_list.append([person_id, ctc, ttc, cts, tts])
    print("step 2")
    SQL = "INSERT INTO persons_metric (person_id, clicksToConvert, timeToConvert, clicksToShare, timeToShare) VALUES (?, ?, ?, ?, ?)"

    # threads can be implemented in order to speed up the process
    start = 0
    n_chunck = 100
    size_chunk = len(persons_list) // n_chunck
    for i in range(n_chunck):
        cursor.executemany(SQL , persons_list[start:start + size_chunk])
        connection.commit()
        print(f"""{i + 1} - commit out of {100}""")
        start += size_chunk
    cursor.executemany(SQL , persons_list[start:len(persons_list)])
    connection.commit()
    