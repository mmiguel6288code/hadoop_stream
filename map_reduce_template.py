from hadoop_stream import HadoopStream

class MapReduce(HadoopStream):

    hdfs_python_path='/usr/bin/python3'
    key_value_delimiter=b'\t'
    hdfs_input_folder_path = ...

    test_input_folder_path = ...
    test_output_file_path = ...
    test_file_limit = 1
    test_line_limit = 100

    def mapper(self,in_lines):
        #Wordcount Example below
        for line in in_lines:
            for word in line.split():
                yield (word,1)

    def reducer(self,mapped):
        #Wordcount Example below
        for word,count_list in mapped:
            yield (word,sum(count_list))


if __name__ == '__main__':
    MapReduce('local_test') 
