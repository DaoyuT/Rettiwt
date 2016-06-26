#!/usr/bin/env python

from app import app

app.config["JSON_SORT_KEYS"] = False
app.run(host='0.0.0.0', port=80, debug = True)
