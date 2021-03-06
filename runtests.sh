#!/bin/bash

set -e

mkdir -p build
cd build

if [ ! -d django ]; then
    git clone 'https://github.com/django/django.git'
fi

cd ..

export DJANGO_SETTINGS_MODULE=demo.settings
export PYTHONPATH=.:build/django

pip -q install mock

$PREFIX build/django/tests/runtests.py $@
