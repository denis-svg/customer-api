from statistics import median, mean
import azure.filters as flt

def create_urls_table(connection):
    sql = f"""
                    CREATE TABLE Urls (
                    id INT PRIMARY KEY IDENTITY NOT NULL,
                    urL VARCHAR(500) NOT NULL,
                    unique_clicks INT,
                    total_clicks INT,
                    timeOnPage INT,
                    timeOnPage_filtered INT,
                    pageBeforeConversion INT,
                    pageBeforeShare INT,
                    ratio_clicks  AS CAST(unique_clicks AS REAL) / CAST(total_clicks AS REAL),
                    ratio_time  AS CAST(timeOnPage_filtered AS REAL) / NULLIF(CAST(timeOnPage AS REAL), 0),
                    UNIQUE(id)
                    );
            """
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()

def insert_urls_table(connection):
    query =         f"""SELECT  events.person_id,
                            events.url,
                            events.clicked_date,
                            events.event_name
                            FROM events
                            ORDER BY events.clicked_date ASC"""

    cursor = connection.cursor()
    cursor.fast_executemany = True
    res = cursor.execute(query).fetchall()
    print("Done querying")
    person_id_dict = {}
    for event in res:
        person_id = event[0]
        url = event[1]
        time = event[2]
        name = event[3]
        if person_id not in person_id_dict:
            person_id_dict[person_id] = [[url, time, name]]
        else:
            person_id_dict[person_id].append([url, time, name])
    
    urls_list = merge(person_id_dict)
    SQL = "INSERT INTO Urls (url, total_clicks, unique_clicks, pageBeforeConversion, pageBeforeShare, timeOnPage, timeOnPage_filtered) VALUES (?, ?, ?, ?, ?, ?, ?)"
    cursor.executemany(SQL, urls_list)
    connection.commit()
    
def totalClicks(person_id_dict):
    urls = {}
    for person_id in person_id_dict.keys():
        for event in person_id_dict[person_id]:
            url = event[0]
            if url not in urls:
                urls[url] = 1
            else:
                urls[url] += 1

    return urls

def uniqueClicks(person_id_dict):
    urls = {}
    for person_id in person_id_dict.keys():
        already_accesed = {}
        for event in person_id_dict[person_id]:
            url = event[0]
            if url not in already_accesed:
                if url not in urls:
                    urls[url] = 1
                else:
                    urls[url] += 1
            already_accesed[url] = True

    return urls

def pageBefore(person_id_dict, event_name):
    urls = {}
    for person_id in person_id_dict.keys():
        previous_url = None
        for event in person_id_dict[person_id]:
            url = event[0]
            name = event[2]
            if name == event_name and previous_url is not None:
                if previous_url not in urls:
                    urls[previous_url] = 1
                else:
                    urls[previous_url] += 1
            previous_url = url

    return urls

def timeOnPage(person_id_dict, filter=False):
    urls = {}
    for person_id in person_id_dict.keys():
        previous = None
        for event in person_id_dict[person_id]:
            time = event[1]
            if previous is not None:
                p_url = previous[0]
                p_time = previous[1]
                if p_url not in urls:
                    urls[p_url] = [ (time - p_time).total_seconds() ]
                else:
                    urls[p_url].append ( (time - p_time).total_seconds() )
            previous = event

    for url in urls.keys():
        # filter
        if filter:
            urls[url] = flt.filter2(urls[url])
        urls[url] = round(mean(urls[url]))
    return urls

def merge(person_id_dict):
    total_clicks = totalClicks(person_id_dict)
    unique_clicks = uniqueClicks(person_id_dict)
    pbc = pageBefore(person_id_dict, 'conversion')
    pbs = pageBefore(person_id_dict, 'share_experience')
    top = timeOnPage(person_id_dict)
    top_filtered = timeOnPage(person_id_dict, filter=True)

    out = []
    for url in total_clicks.keys():
        url_total_clicks = total_clicks[url]
        url_unique_clicks = unique_clicks[url]
        url_pbc = None
        url_pbs = None
        url_top = None
        url_top_filtered = None

        if url in pbc:
            url_pbc = pbc[url]
        if url in pbs:
            url_pbs = pbs[url]
        if url in top:
            url_top = top[url]
        if url in top_filtered:
            url_top_filtered = top_filtered[url]

        out.append([url, url_total_clicks, url_unique_clicks, url_pbc, url_pbs, url_top, url_top_filtered])

    return out