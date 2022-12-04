import requests
import matplotlib.pyplot as plt

URL = "http://127.0.0.1:5000"

def loadStatistics():
    print("Starting loading statistics")
    timestamps = ["lastday", "thisweek", "thismonth"]
    events = ["share", "convert"]
    types = ["time", "clicks"]
    metrics = ["device", "locale"]
    counter = 1
    for type in types:
        for event in events:
            for metric in metrics:
                for timestamp in timestamps:
                    print("Requesting from ", URL + f"""/api/statistics/{type}/{event}/{metric}/{timestamp}""", f" {counter}/{24}")
                    out = requests.get(URL + f"""/api/statistics/{type}/{event}/{metric}/{timestamp}""").json()
                    counter += 1

    print("Finished loading statistics")

def plotStatistics():
    print("Starting loading statistics")
    timestamps = ["lastday", "thisweek", "thismonth"]
    events = ["share", "convert"]
    types = ["time", "clicks"]
    metrics = ["device", "locale"]
    counter = 1
    for type in types:
        for event in events:
            for metric in metrics:
                for timestamp in timestamps:
                    print("Requesting from ", URL + f"""/api/statistics/{type}/{event}/{metric}/{timestamp}""", f" {counter}/{24}")
                    out = requests.get(URL + f"""/api/statistics/{type}/{event}/{metric}/{timestamp}""").json()
                    for key in out.keys():
                        l1 = out[key]["notFiltered"]["values"]
                        l2 = out[key]["filtered"]["values"]
                        plt.plot([i for i in range(len(l1))], l1)
                        plt.plot([i for i in range(len(l2))], l2)
                        plt.title(f"""/api/statistics/{type}/{event}/{metric}/{timestamp}""")
                        plt.xlabel(f"""{key}""")
                        if types == "time":
                            plt.ylabel(f"""seconds""")
                        else:
                            plt.ylabel(f"""clicks""")
                        plt.legend([f"""avg {out[key]["notFiltered"]["mean"]}""", 
                                    f"""avg {out[key]["filtered"]["mean"]}"""], loc ="lower right")
                        plt.show()
                    counter += 1

    print("Finished loading statistics")


loadStatistics()
plotStatistics()