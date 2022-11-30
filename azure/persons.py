import azure.req as req


def create_persons_table(connection):
    sql = f"""                  IF OBJECT_ID(N'dbo.Persons', N'U') IS NULL
                                    BEGIN
                                        CREATE TABLE Persons (
                                            person_id BIGINT PRIMARY KEY NOT NULL,
                                            master_id BIGINT,
                                            locale VARCHAR(6),
                                            device VARCHAR(8),
                                            UNIQUE(person_id)
                                        );
                                    END;
            """
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()

def insert_persons_table(connection, path):
    data = req.Json.load_json(path)
    cursor = connection.cursor()
    cursor.fast_executemany = True

    person_dict = {}
    person_list = []

    for entry in data:
        person = entry['device_profile_id']
        if person not in person_dict:
            person_dict[person] = True

            locale = entry['locale']
            device = entry['device_type']
            master = entry['master_person_id']
            person = int(person)
            if master:
                master = int(master)
            else:
                master = None
            person_list.append([person, master, locale, device])
    
    SQL = "INSERT INTO Persons (person_id, master_id, locale, device) VALUES (?, ?, ?, ?)"

    # threads can be implemented in order to speed up the process
    start = 0
    n_chunck = 100
    size_chunk = len(person_list) // n_chunck
    for i in range(n_chunck):
        cursor.executemany(SQL , person_list[start:start + size_chunk])
        connection.commit()
        print(f"""{i + 1} - commit out of {100}""")
        start += size_chunk
    cursor.executemany(SQL , person_list[start:len(person_list)])
    connection.commit()
