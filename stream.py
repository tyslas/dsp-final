from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink
import time
import sys


def data_transfer():

    input_file = sys.argv[1]
    test_size = sys.argv[2]
    run_num = sys.argv[3]
    file_string = 'file:///home/tito/workspace/inputs/' + str(input_file)
    perf_file = './perf/' + str(test_size) + '/perf_' + str(run_num) + '.csv'
    start = time.time()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.read_text_file(file_string , 'UTF-8')
    ds.add_sink(StreamingFileSink
                .for_row_format('/home/tito/workspace/outputs', SimpleStringEncoder())
                .build())
    
    env.execute('data_transfer_job')
    
    end = time.time()
    perf_file = open(perf_file, 'w+')
    perf_file.write(f'{start},{end}\n') 

if __name__ == '__main__':
    data_transfer()
