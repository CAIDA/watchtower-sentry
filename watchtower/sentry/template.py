import os.path as op

from tornado import template


LOADER = template.Loader(op.join(op.dirname(op.abspath(__file__)), 'templates'), autoescape=None)
TEMPLATES = {
    'graphite': {
        'html': LOADER.load('graphite/message.html'),
        'text': LOADER.load('graphite/message.txt'),
        'short': LOADER.load('graphite/short.txt'),
        'telegram': LOADER.load('graphite/short.txt'),
    },
    'url': {
        'html': LOADER.load('url/message.html'),
        'text': LOADER.load('url/message.txt'),
        'short': LOADER.load('url/short.txt'),
    },
    'common': {
        'html': LOADER.load('common/message.html'),
        'text': LOADER.load('common/message.txt'),
        'short': LOADER.load('common/short.txt'),
    },
    'charthouse': {
        'html': LOADER.load('graphite/message.html'),
        'text': LOADER.load('graphite/message.txt'),
        'short': LOADER.load('graphite/short.txt'),
        'html-batch': LOADER.load('graphite/message-batch.html'),
        'text-batch': LOADER.load('graphite/message-batch.txt'),
        'short-batch': LOADER.load('graphite/short-batch.txt'),
    },
    'emptyresp': {
        'html': LOADER.load('error/message.html'),
        'text': LOADER.load('error/message.txt'),
        'short': LOADER.load('common/short.txt'),
    }
}
TEMPLATES['scanner'] = TEMPLATES['charthouse']
