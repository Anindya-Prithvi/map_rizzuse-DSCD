# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\"F\n\x0cInputMessage\x12\x10\n\x03key\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x12\n\x05value\x18\x02 \x01(\tH\x01\x88\x01\x01\x42\x06\n\x04_keyB\x08\n\x06_value\"\'\n\x07Success\x12\x12\n\x05value\x18\x01 \x01(\tH\x00\x88\x01\x01\x42\x08\n\x06_value25\n\x0fMapProcessInput\x12\"\n\x07Receive\x12\r.InputMessage\x1a\x08.Success28\n\x12ReduceProcessInput\x12\"\n\x07Receive\x12\r.InputMessage\x1a\x08.Successb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _INPUTMESSAGE._serialized_start=18
  _INPUTMESSAGE._serialized_end=88
  _SUCCESS._serialized_start=90
  _SUCCESS._serialized_end=129
  _MAPPROCESSINPUT._serialized_start=131
  _MAPPROCESSINPUT._serialized_end=184
  _REDUCEPROCESSINPUT._serialized_start=186
  _REDUCEPROCESSINPUT._serialized_end=242
# @@protoc_insertion_point(module_scope)
