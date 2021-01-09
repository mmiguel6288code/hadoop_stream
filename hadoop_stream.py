import os.path, sys, os
from itertools import groupby
from operator import itemgetter
from io import BytesIO

class HadoopStream():
    hdfs_python_path='/usr/bin/python3'
    key_value_delimiter=b'\t'
    test_input_folder_path = ...
    test_output_file_path = ...
    hdfs_input_folder_path = ...
    test_file_limit = 1
    test_line_limit = 100

    def __init__(self,mode):
        self.mode = mode
        if mode == 'mapper':
            self._do_mapper()
        elif mode == 'reducer':
            self._do_reducer()
        elif mode == 'local_test':
            self._do_local_test()
        elif mode == 'multi':
            self._do_multi()
        else:
            raise Exception('Invalid mode: %s' % repr(mode))
    def _do_mapper(self):
        alt_stdin = open(0,'rb')
        try:
            for key,value in self.mapper(self.alt_stdin):
                sys.stdout(b''.join([key,self.key_value_delimiter,value,b'\n']))
        finally:
            alt_stdin.close()
    def _do_reducer(self):
        alt_stdin = open(0,'rb')
        def get_kvp(self=self,alt_stdin=alt_stdin):
            for line in alt_stdin:
                key,value = line.split(self.key_value_delimiter)
                yield (key,value)

        mapped = groupby(get_kvp(),itemgetter(0))
        try:
            for out_key,out_value in self.reducer(mapped):
                sys.stdout(b''.join([out_key,self.key_value_delimiter,out_value,b'\n']))
        finally:
            alt_stdin.close()
    def _do_local_test(self):
        #test both mapper/reducer on same machine
        local_buffer = BytesIO()
        def in_lines(self=self):

            num_files = 0
            num_lines = 0
            for file_name in os.listdir(self.test_input_folder_path):
                with open(os.path.join(self.test_input_folder_path,file_name),'rb') as f:
                    for line in f:
                        yield line
                        num_lines += 1
                        if num_lines > self.test_line_limit:
                            break
                num_files += 1
                if num_files > self.test_file_limit:
                    break

        for key,value in self.mapper(in_lines):
            local_buffer.write(b''.join([key,self.key_value_delimiter,value,b'\n']))

        def get_kvp(self=self,local_buffer=local_buffer):
            for line in local_buffer:
                key,value = line.split(self.key_value_delimiter)
                yield (key,value)
        mapped = groupby(get_kvp(),itemgetter(0))
        with open(self.test_output_filepath,'wb') as f:
            for out_key,out_value in self.reducer(mapped):
                f.write(b''.join([out_key,self.key_value_delimiter,out_value,b'\n']))

    def _do_multi(self):
        ...

    def mapper(self,in_lines):
        ...
        raise NotImplementedError

        #Wordcount Example below
        for line in in_lines:
            for word in line.split():
                yield (word,1)

    def reducer(self,mapped):
        ...
        raise NotImplementedError

        #Wordcount Example below
        for word,count_list in mapped:
            yield (word,sum(count_list))

    def create_files(self):
        directory,file_name = os.path.split(__file__)
        module_name = file_name[:-3]
        class_name = self.__class__.__name__
        for role in ['mapper','reducer']:
            with open(os.path.join(directory,role+'.py'),'w') as f:
                f.write('''{hdfs_python_path}
from {module_name} import {class_name}
{class_name}('{role}')'''.format(hdfs_python_path=self.hdfs_python_path,module_name=module_name,class_name=class_name,role=role))
            #chmod + x
    def copy_input_files(self):
        ...
    def start_job(self):
        ...

