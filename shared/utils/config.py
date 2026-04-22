# -*- coding: utf-8 -*-
# shared/utils/config.py

import yaml

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)