#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import sys, random

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print('argument error')
		exit(1)
	cnt = int(sys.argv[1]) * (1024 ** 3)
	f = open(sys.argv[2], 'w')
	while cnt > 0:
		x = random.randint(10000, 99999)
		s = 'http://pingcap.com/{}\n'.format(x)
		cnt -= len(s)
		f.write(s)