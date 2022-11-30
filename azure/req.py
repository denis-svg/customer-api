import json

class Json:
    @staticmethod
    def save_json(data, path):
        with open(path, "w") as outfile:
            json.dump(data, outfile)

    @staticmethod
    def load_json(path):
        f = open(path)
        data = json.load(f)
        f.close()
        return data