#!/bin/bash
export ANSIBLE_STDOUT_CALLBACK=debug
ansible-playbook -i ./vhosts $1
