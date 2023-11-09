### Setup Virtual env

```
python3.9 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

### Usage
python main.py [config file]

see `configs/example.yml` for config file options



### VSCode setup to run

To reade the testing config file, 
add the following config to `.vscode/launch.json`

```        
{
    "name": "main.py",
    "type": "python",
    "request": "launch",
    "program": "main.py",
    "console": "integratedTerminal",
    "justMyCode": true,
    "args": ["configs/test.yml"],
}
```

### MQTT Topics

`/ping/<asset>` all ping tags from all machines.   

`/counter/<asset>` all counter tags for one asset

