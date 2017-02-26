#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
# 己方库
import quoradaily as qd


app = Flask(__name__)


@app.route('/select/')
@app.route('/select/<int:page>')
def SelectPage(page=1):
    db = qd.Database()
    status, limit = qd.ST_PASS, 10
    skip = max(0, page - 1) * limit
    items = list(db.find_raw(status, skip, limit))
    return render_template('select.html', items=items)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')