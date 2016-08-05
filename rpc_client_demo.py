#!/usr/bin/env python
# ~*~ coding: utf-8 ~*~
#

from __future__ import absolute_import, unicode_literals
from rpc_server_demo import app

app.make_connection()

print(app.rpc_server.callback_map)
print(app.connection)

a = app.send_rpc('cmdb', func='asset_list', method='GET', args=('asdf', 'asdf'), kwargs={})
print(a)


