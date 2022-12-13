import json

with open("./env.json", 'r') as load_f:
    d = json.load(load_f)
    print(d)
    print(type(d))
    print(d["namenode"])
