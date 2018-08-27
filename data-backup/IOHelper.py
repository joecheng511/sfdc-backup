# -*- coding: utf-8 -*-

import os
import shutil

class IOHelper:

    @staticmethod
    def init():
        IOHelper.makeClearDir('log')
        IOHelper.makeClearDir('output')

    @staticmethod
    def makeClearDir(dirName):
        strDirName = str(dirName)
        if os.path.isdir(strDirName):
            shutil.rmtree(strDirName)
        os.makedirs(strDirName)

    @staticmethod
    def appendToLog(logName, message):
        with open("log/{}".format(str(logName)), "a") as file:
            file.write(str(message))

    @staticmethod
    def outputObjectToFile(objectName, text):
        with open("output/{}.csv".format(objectName), "a") as text_file:
            text_file.write(text)
