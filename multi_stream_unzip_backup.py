import os
import json
import gzip
import stream_unzip
from io import BytesIO
import multiprocessing as mp
import time
import tqdm
import argparse
from goose3 import Goose
from goose3.text import StopWordsChinese

_LOCAL_FILE_HEADER_ = b"PK\x03\x04"


class MultiUnzip:
    def __init__(
        self,
        file_path: os.PathLike,
        output_path: os.PathLike,
        password: bytes,
        num_proc: int = 1,
        max_size: int = 600,
        rate: int = 100,
    ) -> None:
        """
        Argments:
         - `num_proc` : the number of process for data stream reader, namely `read_bin`
         - `rate` : the rate from process number of `read_bin` divided by `dump_str`
        """
        (
            self.file_path,
            self.output_path,
            self.password,
            self.lines_num_proc,
            self.dump_num_proc,
        ) = (file_path, output_path, password, num_proc, rate * num_proc)
        self.quene = mp.Manager().Queue(max_size)
        self.rate = rate

    def read_bin(self, id_proc: int, min_byte_size: int, offset: int, proc_size: int):
        """
        Accquire binary data from raw zip files

        Argments:
         - `offset` : the byte position in file IO.
         - `proc_size` : the divided bytesize of part in file IO.
        """
        with open(self.file_path, "rb") as f:
            byte_lines = []
            if id_proc > 0 or offset > 0:
                f.seek(proc_size * id_proc + offset)
                for line in f:
                    if _LOCAL_FILE_HEADER_ in line:
                        line_splits = line.split(_LOCAL_FILE_HEADER_)
                        byte_lines = [_LOCAL_FILE_HEADER_, line_splits[1]]
                        break
            else:
                byte_lines = [f.readline()]
            while f.tell() < (id_proc + 1) * proc_size + offset:
                print(
                    f"\nRead process {id_proc} starts, position offsetting in {f.tell()}."
                )
                for line in f:
                    if _LOCAL_FILE_HEADER_ in line:
                        if byte_lines.__sizeof__() < min_byte_size:
                            byte_lines.append(line)
                            continue
                        end_lines = line.split(_LOCAL_FILE_HEADER_)
                        byte_lines.append(end_lines[0])
                        self.quene.put(byte_lines)
                        byte_lines = [_LOCAL_FILE_HEADER_, b"".join(end_lines[1:])]
                        break
                    else:
                        byte_lines.append(line)
                while self.quene.full():
                    print(f"\nQuene is full, read process {id_proc} now rests...")
                    time.sleep(10)
        self.quene.put(byte_lines)
        print(
            f"\n*************** Read process {id_proc} will soon terminate ***************"
        )
        i = 0
        while i < self.rate:
            if not self.quene.full():
                self.quene.put(b"EOF")
                i += 1
        print(f"\n*************** Read process {id_proc} terminates ***************")

    def dump_str(self, filename: str, id_proc: int) -> None:
        """
        Unzip the binary format data into json format strings and dump into text IO.
        """
        exclude_write_line_num = 0
        failure_write_line_num = 0
        goose = Goose({"stopwords_class": StopWordsChinese, "parser": "trafilatura"})
        while True:
            try:
                byte_lines = self.quene.get()
            except:
                continue
            if byte_lines == b"EOF":
                break
            print(
                f"\n===================== Dump process {id_proc:03d} is now working ====================="
            )
            with open(os.path.join(self.output_path, "exception.txt"), "a") as f_:

                def json_dict_iterator(byte_lines, password, goose):
                    nonlocal failure_write_line_num, exclude_write_line_num
                    for _file_name, _file_size, unzipped_chunks in tqdm.tqdm(
                        stream_unzip.stream_unzip(byte_lines, password),
                        desc=f"The lines of {id_proc:03d} file is",
                        unit=" lines",
                    ):
                        # unzipped_chunks must be iterated to completion or UnfinishedIterationError will be raised
                        try:
                            unzipped_data = b"".join(unzipped_chunks)
                            chunks_size = len(unzipped_data)
                            if chunks_size < 512:
                                exclude_write_line_num += 1
                                continue
                            with gzip.GzipFile(fileobj=BytesIO(unzipped_data)) as gz_f:
                                uncompressed_data = gz_f.read()
                                json_data = json.loads(uncompressed_data)
                                raw_html = json_data["content_noencode"]
                                if len(raw_html) < 10:
                                    exclude_write_line_num += 1
                                    continue
                                article = goose.extract(raw_html=raw_html)
                                if article.cleaned_text == "":
                                    exclude_write_line_num += 1
                                    continue
                                try:
                                    json_out = {
                                        "meta": {
                                            "type": "5.6T",
                                            "desc": json_data["desc"],
                                            "title": json_data["title"],
                                            "url": json_data["link"],
                                            "signature": json_data["signature"],
                                            "lang": json_data["lang"],
                                            "create_time": json_data["create_time"],
                                            "alias": json_data["alias"],
                                        },
                                        "text": article.cleaned_text,
                                    }
                                except:
                                    json_out = {
                                        "meta": {
                                            "type": "5.6T",
                                            "desc": json_data["desc"],
                                            "title": json_data["title"],
                                            "url": json_data["link"],
                                            "signature": json_data["signature"],
                                            "lang": json_data["lang"],
                                            # "create_time": json_data["create_time"],
                                            "alias": json_data["alias"],
                                        },
                                        "text": article.cleaned_text,
                                    }
                                yield json.dumps(json_out, ensure_ascii=False) + "\n"
                        except stream_unzip.TruncatedDataError:
                            break
                        except Exception as e:
                            print(
                                f"\nDump process {id_proc} has met an exception : {e}"
                            )
                            f_.write(
                                f"{time.strftime('%Y/%D %H:%M:%S')}, dump process {id_proc}raised an exception :{e}\n"
                            )
                            failure_write_line_num += 1
                            continue

                with open(
                    os.path.join(
                        self.output_path,
                        filename + f".id.{id_proc:03d}.time.{time.time():.0f}.jsonl",
                    ),
                    mode="w",
                ) as writer:
                    try:
                        writer.writelines(
                            json_dict_iterator(byte_lines, self.password, goose)
                        )
                    except stream_unzip.TruncatedDataError:
                        pass
            print(
                f"\n================ Dump process {id_proc} finishes, waiting for next boot ================"
            )
        print(
            f"\n------------------- Dump process {id_proc} terminated -------------------"
        )
        with open(os.path.join(self.output_path, "failure.txt"), "a") as f:
            print(
                f"Dump process {id_proc} has accumulated {failure_write_line_num} failure write lines, {exclude_write_line_num} excluded write lines",
                file=f,
                flush=True,
            )

    def run(
        self,
        min_byte_size: int,
    ):
        """
        Argments:
         - `min_byte_size` : the minimal bytesize of buffer to dump.
        """
        # We can use a with statement to ensure threads are cleaned up promptly
        filename = os.path.basename(self.file_path)
        lines_proc = mp.Pool(processes=self.lines_num_proc)
        with open(self.file_path, "rb") as f:
            # # 把前八分之七的内容并行
            proc_size = os.fstat(f.fileno()).st_size // self.lines_num_proc
        for id_proc in range(self.lines_num_proc):
            lines_proc.apply_async(
                func=self.read_bin,
                args=(id_proc, min_byte_size, 0, proc_size),
            )
        dump_proc = mp.Pool(processes=self.dump_num_proc)
        for id_proc in range(self.dump_num_proc):
            dump_proc.apply_async(
                func=self.dump_str,
                args=(filename, id_proc),
            )
        lines_proc.close()
        dump_proc.close()

        lines_proc.join()
        dump_proc.join()
        print(
            f"\n>>>>>>>>>>>>>>>>> All the processes terminate, unzipping file {self.file_path} successfully <<<<<<<<<<<<<<<<"
        )


passwords = {
    "202201.zip": b"12QWaszx",
    "202111.zip": b"12QWaszx",
    "202212.zip": b"gUX1joCSt7NP",
    "202211.zip": b"gUX1joCSt7NP",
    "202209.zip": b"12QWaszx",
    "202210.zip": b"gUX1joCSt7NP",
    "202208,zip": b"gUX1joCSt7NP",
    "202207.zip": b"gUX1joCSt7NP",
    "202208.zip": b"gUX1joCSt7NP",
    "202206.zip": b"gUX1joCSt7NP",
    "202205.zip": b"12QWaszx",
    "202204.zip": b"12QWaszx",
    "202203.zip": b"12QWaszx",
    "202202.zip": b"gUX1joCSt7NP",
    "202112.zip": b"gUX1joCSt7NP",
    "202301.zip": b"gUX1joCSt7NP",
}
# file_path = "/mnt/cos/cos_shanghai_2/raw_datasets/mt/202212.zip"
# file_path = "/dataset_goosefs/cos_shanghai_2/raw_datasets/mt/p3/p1/202211.zip"
# file_path = "/data_turbo/home/chenbofei/code/test_data.zip"
# password = b"test_password"
output_path = "/dataset_cosfs/raw_datasets/mt_unzip_jsonl_1"
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process arguments.")
    parser.add_argument(
        "-n",
        "--num_proc",
        type=int,
        default=8,
        help="number of data stream reader processors",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        default="./output",
        help="folder for output data",
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=int,
        default=100,
        help="the rate from process number of read process divided by write process",
    )
    parser.add_argument(
        "-m",
        "--min_byte_size",
        type=int,
        default=None,
        help="minimal bytesize for string dumping, unit : MB",
    )
    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        help="zip file data",
    )
    args = parser.parse_args()
    output_path = output_path if output_path else args.output_path
    file_path = args.file_path
    password = passwords[os.path.basename(file_path)]
    num_proc, min_bytes_size, rate = (
        args.num_proc,
        args.min_byte_size,
        args.rate,
    )
    min_byte_size = (
        args.min_byte_size * 1024 * 1024 if args.min_byte_size != None else 0
    )
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    output_dir = os.path.join(
        output_path, os.path.basename(file_path).replace(".zip", "")
    )
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    MultiUnzip(
        file_path=file_path,
        password=password,
        output_path=output_dir,
        num_proc=num_proc,
        rate=rate,
    ).run(min_byte_size)
