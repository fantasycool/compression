package compression

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/colinmarc/hdfs"
	"io"
	"log"
	"os"
	"oss"
	"strings"
	"time"
)

type F struct {
	f  *os.File
	gz *gzip.Writer
	fw *bufio.Writer
}

func CreateGZ(fileName string) (f F, err error) {
	fi, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Printf("Create Gz file failed! err message:%s \n", err)
	}
	gz := gzip.NewWriter(fi)
	fw := bufio.NewWriter(gz)
	f = F{f: fi, gz: gz, fw: fw}
	return f, nil
}

func WriteGZ(f F, s string) (int, error) {
	n, err := f.fw.WriteString(s)
	if err != nil {
		log.Printf("Write failed err:%s! \n", err)
		return n, err
	}
	f.fw.Flush()
	return n, nil
}

func HadoopReadWriteGzipFile(f F, h HadoopFile, hdfsClient *hdfs.Client) (int64, error) {
	hdfsPath := h.dirPrefix + h.fileInfo.Name()
	hdfsReader, err := hdfsClient.Open(hdfsPath)
	if err != nil {
		log.Printf("hdfsClient opend failed! err message: %s\n", err)
		return 0, err
	}
	defer hdfsReader.Close()
	log.Printf("start to use io Copy \n")
	written, err := io.Copy(f.fw, hdfsReader)
	return written, err
}

//after call this method,remember to close [Reader,Writer]
func LocalFileReadWriteOssFile(f F, h HadoopFile, ossClient *oss.Client,
	bucket string, ossDir string, b bool) error {
	timeStamp := fmt.Sprintf("%d", makeTimestamp())
	ossPath := ossDir + timeStamp + f.f.Name()
	hdfsPath := h.dirPrefix + h.fileInfo.Name()
	if b {
		dateStr, err := getDateStamp(hdfsPath)
		if err != nil {
			log.Printf("get date info from hdfsPath %s failed!", hdfsPath)
			return err
		}
		ossNameArray := strings.Split(f.f.Name(), "/")
		ossPath = dateStr + "/" + timeStamp + ossNameArray[(len(ossNameArray)-1)]
		if err != nil {
			return err
		}
	}
	fi, err := os.Open(f.f.Name())
	log.Printf("Open file %s \n", f.f.Name())
	defer fi.Close()
	if err != nil {
		return err
	}
	err = ossClient.PutObject(bucket, ossPath, fi, nil)
	if err != nil {
		log.Printf("Oss Put Object failed %s!\n", err)
		return err
	} else {
		log.Printf("ossDir:%s put file:%s success!! \n", ossDir, f.f.Name())
	}
	return nil
}

func StrWriteToOssFile(str string, ossClient *oss.Client, bucket string, ossDir string) error {
	timeStamp := fmt.Sprintf("%d", makeTimestamp())
	filePath := fmt.Sprintf("%s%s", "/tmp/", timeStamp)
	f, err := CreateGZ(filePath)
	defer os.Remove(filePath)
	if err != nil {
		return err
	}
	n, err := WriteGZ(f, str)
	if err != nil {
		return err
	}
	log.Printf("Has written %d bytes to temp file \n", n)
	err = CloseGZ(f)
	if err != nil {
		return err
	}
	ossPath := ossDir + time.Now().Format("20060102") + "/" + timeStamp + ".gz"
	err = WriteFileToOss(filePath, timeStamp, ossClient, ossPath, bucket)
	if err != nil {
		log.Printf("Write to file failed %s !", err)
		return err
	}
	err = os.Remove(filePath)
	if err != nil {
		fmt.Printf("remove failed! err:%s", err)
	}
	return nil
}

func WriteFileToOss(filePath string, fileName string, ossClient *oss.Client, ossPath string, bucket string) error {
	fi, err := os.Open(filePath)
	defer fi.Close()
	if err != nil {
		return err
	}
	err = ossClient.PutObject(bucket, ossPath, fi, nil)
	if err != nil {
		log.Printf("Oss Put Object failed %s!\n", err)
		return err
	} else {
		return nil
	}
}

func CloseGZ(f F) error {
	err := f.fw.Flush()
	err = f.gz.Flush()
	err = f.gz.Close()
	err = f.f.Close()
	return err
}

func CompressString(str string) (*bytes.Buffer, error) {
	var b *bytes.Buffer = new(bytes.Buffer)
	gz := gzip.NewWriter(b)
	_, err := gz.Write([]byte(str))
	if err != nil {
		return nil, err
	}
	if err := gz.Flush(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	log.Printf("format%d \n", b.Len())
	return b, nil
}
