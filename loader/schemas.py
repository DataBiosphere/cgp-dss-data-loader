
# These schemas are based on the DOS schemas with minor modifications. See
# https://github.com/ga4gh/data-object-service-schemas/blob/master/openapi/data_object_service.swagger.yaml#L594

data_object_schema = {
    'type': 'object',
    'required': ['id', 'size', 'created', 'checksums', 'urls'],
    'properties': {
        'id': {
            'type': 'string',
            'description': 'REQUIRED\n'
                           'An identifier unique to this Data Object.'
        },
        'name': {
            'type': 'string',
            'description': 'OPTIONAL\n'
                           'A string that can be optionally used to name a Data Object.'
        },
        'size': {
            'type': 'string',
            'format': 'int64',
            'description': 'REQUIRED\n'
                           'The computed size in bytes.'
        },
        'created': {
            'type': 'string',
            'format': 'date-time',
            'description': 'REQUIRED\n'
                           'Timestamp of object creation in RFC3339.'
        },
        'updated': {
            'type': 'string',
            'format': 'date-time',
            'description': 'OPTIONAL\n'
                           'Timestamp of update in RFC3339, identical to create timestamp in systems\n'
                           'that do not support updates.'
        },
        'version': {
            'type': 'string',
            'description': 'OPTIONAL\n'
                           'A string representing a version.'
        },
        'mime_type': {
            'type': 'string',
            'description': 'OPTIONAL\n'
                           'A string providing the mime-type of the Data Object.\n'
                           'For example, \'application/json\'.'
        },
        'checksums': {
            'type': 'array',
            'items': {
                "type": "object",
                "properties": {
                    "checksum": {
                        "type": "string",
                        "description": "REQUIRED\n"
                                       "The hex-string encoded checksum for the Data."
                    },
                    "type": {
                        "type": "string",
                        "description": "OPTIONAL\n"
                                       "The digest method used to create the checksum. If left unspecified md5\n"
                                       "will be assumed.\n"
                                       "\n"
                                       "possible values:\n"
                                       "md5                # most blob stores provide a checksum using this\n"
                                       "multipart-md5      # multipart uploads provide a specialized tag in S3\n"
                                       "sha256\n"
                                       "sha512"
                    }
                }
            },

            'description': 'REQUIRED\n'
                           'The checksum of the Data Object. At least one checksum must be provided.'
        },
        'urls': {
            'type': 'array',
            'items': {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "REQUIRED\n"
                                       "A URL that can be used to access the file."
                    },
                    "system_metadata": {
                        "type": "object",
                        "additionalProperties": True,
                        "description": "OPTIONAL\n"
                                       "These values are reported by the underlying object store.\n"
                                       "A set of key-value pairs that represent system metadata about the object."
                    },
                    "user_metadata": {
                        "type": "object",
                        "additionalProperties": True,
                        "description": "OPTIONAL\n"
                                       "A set of key-value pairs that represent metadata provided by the uploader."
                    }
                }
            },
            'description': 'OPTIONAL\n'
                           'The list of URLs that can be used to access the Data Object.'
        },
        'description': {
            'type': 'string',
            'description': 'OPTIONAL\n'
                           'A human readable description of the contents of the Data Object.'
        },
        'aliases': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'description': "OPTIONAL\n"
                           "A list of strings that can be used to find this Data Object.\n"
                           "These aliases can be used to represent the Data Object's location in\n"
                           "a directory (e.g. \'bucket/folder/file.name\') to make Data Objects\n"
                           "more discoverable."
        }
    }
}

data_bundle = {
    "type": "object",
    # TODO: required, but we don't have checksums for bundles
    'required': ['data_object_ids', 'created', 'updated', 'version'],  # , 'checksums'],
    "properties": {
        "id": {
            "type": "string",
            "description": "REQUIRED\n"
                           "An identifier, unique to this Data Bundle"
        },
        "data_object_ids": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "REQUIRED\n"
                           "The list of Data Objects that this Data Bundle contains."
        },
        "created": {
            "type": "string",
            "format": "date-time",
            "description": "REQUIRED\n"
                           "Timestamp of object creation in RFC3339."
        },
        "updated": {
            "type": "string",
            "format": "date-time",
            "description": "REQUIRED\n"
                           "Timestamp of update in RFC3339, identical to create timestamp in systems\n"
                           "that do not support updates."
        },
        "version": {
            "type": "string",
            "description": "REQUIRED\n"
                           "A string representing a version, some systems may use checksum, a RFC3339\n"
                           "timestamp, or incrementing version number. For systems that do not support\n"
                           "versioning please use your update timestamp as your version."
        },
        'checksums': {
            'type': 'array',
            'items': {
                "type": "object",
                "properties": {
                    "checksum": {
                        "type": "string",
                        "description": "REQUIRED\n"
                                       "The hex-string encoded checksum for the Data."
                    },
                    "type": {
                        "type": "string",
                        "description": "OPTIONAL\n"
                                       "The digest method used to create the checksum. If left unspecified md5\n"
                                       "will be assumed.\n"
                                       "\n"
                                       "possible values:\n"
                                       "md5                # most blob stores provide a checksum using this\n"
                                       "multipart-md5      # multipart uploads provide a specialized tag in S3\n"
                                       "sha256\n"
                                       "sha512"
                    }
                }
            },
            "description": "REQUIRED\n"
                           "At least one checksum must be provided.\n"
                           "The data bundle checksum is computed over all the checksums of the\n"
                           "Data Objects that bundle contains."
        },
        "description": {
            "type": "string",
            "description": "OPTIONAL\n"
                           "A human readable description."
        },
        "aliases": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "OPTIONAL\n"
                           "A list of strings that can be used to identify this Data Bundle."
        },
        "system_metadata": {
            "type": "object",
            "additionalProperties": True,
            "description": "OPTIONAL\n"
                           "These values are reported by the underlying object store.\n"
                           "A set of key-value pairs that represent system metadata about the object."
        },
        "user_metadata": {
            "type": "object",
            "additionalProperties": True,
            "description": "OPTIONAL\n"
                           "A set of key-value pairs that represent metadata provided by the uploader."
        }
    }
}

standard_schema = {
    'type': 'object',
    'properties': {
        'data_objects': {
            'type': 'object',
            'patternProperties': {
                # Note: does not match regexes with capitals
                '^.*/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$': data_object_schema,
            }
        },
        "data_bundle": data_bundle
    }
}
