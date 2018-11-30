#!/usr/bin/env python
""" 
Tencent is pleased to support the open source community by making Tencent ML-Images available.
Copyright (C) 2018 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
https://opensource.org/licenses/BSD-3-Clause
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and limitations under the License.
"""

import os
import sys
import urllib
import argparse
import threading, signal
import time
import itertools
import socket
import cv2
import time
from datetime import timedelta

socket.setdefaulttimeout(10.0)


class WordTrie:
    def __init__(self, save_file=None):
        self.trie = dict()
        if save_file is not None:
            self.parse_file(save_file)

    def check(self, name):
        head = self.trie
        for c in name:
            if head == -1 or c not in head:
                return False
            else:
                head = head[c]
        if head == -1 or -1 in head:
            return True
        else:
            return False

    def reset(self):
        self.trie.clear()

    def add(self, name):
        head = self.trie
        for c in name[:-1]:
            if c not in head:
                head[c] = dict()
            elif head[c] == -1:
                head[c] = {-1: None}
            head = head[c]
        c = name[-1]
        if c not in head:
            head[c] = -1
        else:
            if head[c] == -1 or -1 in head[c]:
                print name
                raise Exception("duplicate name added")
            head[c][-1] = None


    def parse_file(self, save_file):
        try:
            count = 0
            with open(save_file, "r") as f:
                for line in f:
                    count += 1
                    im_name = line.split("\t")[0]
                    im_name = im_name.split(".")[0]
                    self.add(im_name)
                print "parsed %d records" % count
        except IOError:
            print("No save records found!")


def downloadImg(start, end, url_list, save_dir, name_trie, short_limit=448.):
    global record, count, count_invalid, is_exit
    line_num = start - 1
    begin_time = time.time()
    with open(url_list, 'r')  as url_f:
        for line in itertools.islice(url_f, start, end):
            line_num += 1
            sp = line.rstrip('\n').split('\t')
            url = sp[0]
            im_name = url.split('/')[-1]
            format = im_name.split(".")[-1]
            save_name = str(line_num) + "." + format
            if name_trie.check(str(line_num)):
                record += 1
                continue
            try:
                save_path = os.path.join(save_dir, save_name)
                urllib.urlretrieve(url, save_path)
                img = cv2.imread(save_path)
                if img is None:
                    raise IOError()
                h, w = img.shape[0], img.shape[1]
                if w > short_limit and h > short_limit:
                    if w > h:
                        new_w, new_h = int(short_limit * w / h), int(short_limit)
                    else:
                        new_w, new_h = int(short_limit), int(short_limit * h / w)
                    img = cv2.resize(img, (new_w, new_h))
                    # cv2.imshow("resized", img)
                    cv2.imwrite(save_path, img)
                record += 1
                im_file_Record.write(save_name + "\t" + line)
                name_trie.add(str(line_num))
                print('{} \t line_num = {}\turl = {} is finished and {} imgs have been downloaded of all {} imgs'
                      .format(str(timedelta(seconds=time.time() - begin_time)), line_num, url, record, count))
            except:
                print ("{} \t line_num = {}\tThe url:{} is ***INVALID***".format(str(timedelta(seconds=time.time() - begin_time)), line_num, url))
                invalid_file.write(save_name + "\t" + line)
                count_invalid += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--url_list', type=str, help='the url list file', default='train_urls_tiny.txt')
    parser.add_argument('--im_list', type=str, default='img.txt', help='the image list file')
    parser.add_argument('--num_threads', type=int, default=8, help='the num of processing')
    parser.add_argument('--save_dir', type=str, default='./test_2_images', help='the directory to save images')
    args = parser.parse_args()

    url_list = args.url_list
    im_list = args.im_list
    num_threads = args.num_threads
    save_dir = args.save_dir
    # create savedir
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)

    count = 0  # the num of urls
    count_invalid = 0  # the num of invalid urls
    record = 0
    with open(url_list, 'r') as f:
        for line in f:
            count += 1
    part = int(count / num_threads)
    finish_trie = WordTrie(save_file=im_list)
    finish_trie.parse_file("invalid_url.txt")
    with open(im_list, 'a') as im_file_Record, open('invalid_url.txt',
                                                    'a') as invalid_file:  # record the downloaded imgs
        thread_list = []
        for i in range(num_threads):
            part_save_dir = os.path.join(save_dir, str(i))
            if not os.path.isdir(part_save_dir):
                os.mkdir(part_save_dir)
            if (i == num_threads - 1):
                t = threading.Thread(target=downloadImg,
                                     kwargs={'name_trie': finish_trie, 'start': i * part, 'end': count,
                                             'url_list': url_list, 'save_dir': part_save_dir})
            else:
                t = threading.Thread(target=downloadImg,
                                     kwargs={'name_trie': finish_trie, 'start': i * part, 'end': (i + 1) * part,
                                             'url_list': url_list, 'save_dir': part_save_dir})
            t.setDaemon(True)
            thread_list.append(t)
            t.start()

        for i in range(num_threads):
            try:
                while thread_list[i].isAlive():
                    pass
            except KeyboardInterrupt:
                break

        if count_invalid == 0:
            print ("all {} imgs have been downloaded!".format(count))
        else:
            print(
            "{}/{} imgs have been downloaded, {} URLs are invalid".format(count - count_invalid, count, count_invalid))
