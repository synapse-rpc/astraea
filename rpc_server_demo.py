#!/usr/bin/env python
# ~*~ coding: utf-8 ~*~
#

from __future__ import absolute_import, unicode_literals
import logging
from base import Synapse

logging.basicConfig(level=logging.INFO)

app = Synapse('cmdb', app_id='BSDS')


@app.rpc_server.callback(methods=['GET', 'POST'])
def asset_list(asset_name, asset_id, zone="hz"):

    return "%s.%s zone: %s" % (asset_name, asset_id, zone)


if __name__ == '__main__':

    print(app.rpc_server.callback_map)
    app.run(process_num=10)
