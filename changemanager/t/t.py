import yaml

with open("t.yml") as f:
    config=yaml.safe_load(f)

print(config)
