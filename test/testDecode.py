import os
import gzip

target = '../Assets/b9f8f400-2961-49be-8585-9bc2596de685_output_1774267070713.txt.gz'
output_path = os.path.join('./', 'b9f8f400-2961-49be-8585-9bc2596de685_output_1774267070713.txt')
with gzip.open(target, 'rb') as f_in:
    with open(output_path, 'wb') as f_out:
        f_out.writelines(f_in)