# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: database.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0e\x64\x61tabase.proto\" \n\x0f\x64\x61tabaseRequest\x12\r\n\x05query\x18\x01 \x01(\t\"\'\n\x10\x64\x61tabaseResponse\x12\x13\n\x0b\x64\x62_response\x18\x01 \x01(\x0c\x32\x81\x01\n\x08\x64\x61tabase\x12\x36\n\rqueryDatabase\x12\x10.databaseRequest\x1a\x11.databaseResponse\"\x00\x12=\n\x14\x65xecuteClientRequest\x12\x10.databaseRequest\x1a\x11.databaseResponse\"\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'database_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _DATABASEREQUEST._serialized_start=18
  _DATABASEREQUEST._serialized_end=50
  _DATABASERESPONSE._serialized_start=52
  _DATABASERESPONSE._serialized_end=91
  _DATABASE._serialized_start=94
  _DATABASE._serialized_end=223
# @@protoc_insertion_point(module_scope)
