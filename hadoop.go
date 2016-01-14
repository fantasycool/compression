package compression

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"io"
	"log"
	"os"
	"oss"
	"regexp"
	"time"
)

type OssWrite struct {
	hdfsDir string
	ossDir  string
	data    string
}

type HadoopFile struct {
	dirPrefix string
	fileInfo  os.FileInfo
}

func (h *HadoopFile) GetDirPrefix() string {
	return h.dirPrefix
}

func (h *HadoopFile) GetFileInfo() os.FileInfo {
	return h.fileInfo
}

func listAllDirFiles(hdfsDir string, hdfsClient *hdfs.Client) ([]HadoopFile, error) {
	hdfsFileInfos, err := RecurseInfos(hdfsClient, hdfsDir)
	if err != nil {
		return nil, err
	}
	return hdfsFileInfos, nil
}

func RecurseInfos(hdfsClient *hdfs.Client, hdfsDir string) ([]HadoopFile, error) {
	hdfsFiles := make([]HadoopFile, 0)
	dirs, err := hdfsClient.ReadDir(hdfsDir)
	if err != nil {
		return nil, err
	}
	for _, fileInfo := range dirs {
		if fileInfo.IsDir() {
			recurseInfos, err := RecurseInfos(hdfsClient, hdfsDir+fileInfo.Name()+"/")
			if err != nil {
				return nil, err
			}
			for _, f := range recurseInfos {
				hdfsFiles = append(hdfsFiles, f)
			}
		} else {
			f := HadoopFile{dirPrefix: hdfsDir, fileInfo: fileInfo}
			hdfsFiles = append(hdfsFiles, f)
		}
	}
	return hdfsFiles, nil
}

func ReadHdfsWriteOSS(hdfsDir string, ossDir string, bucket string,
	hdfsClient *hdfs.Client, ossClient *oss.Client, cm bool) (int, error) {
	if ossDir[len(ossDir)-1:] != "/" {
		return 0, fmt.Errorf("ossDir:%s parameter have to be / in the end!", ossDir)
	}
	if hdfsDir[len(hdfsDir)-1:] != "/" {
		return 0, fmt.Errorf("hdfsDir:%s parameter have to be / in the end!", hdfsDir)
	}
	hadoopFiles, err := listAllDirFiles(hdfsDir, hdfsClient)
	log.Printf("%d num hadoop files should be copy to !", len(hadoopFiles))
	if err != nil {
		return 0, nil
	}
	countNum := 0
	for _, h := range hadoopFiles {
		//if use gzip
		if cm {
			err := gzWriteToOss(h, hdfsClient, ossClient, ossDir, bucket, false)
			if err != nil {
				return 0, err
			}
		} else {
			err := writeToOss(h, ossDir, hdfsClient, ossClient, bucket, false)
			if err != nil {
				return 0, err
			}
		}
		countNum++
	}
	return countNum, nil
}

func gzWriteToOss(h HadoopFile, hdfsClient *hdfs.Client, ossClient *oss.Client, ossDir string, bucket string, b bool) error {
	f, err := CreateGZ("/tmp/" + h.fileInfo.Name() + ".gz")
	n, err := HadoopReadWriteGzipFile(f, h, hdfsClient)
	log.Printf("HadoopReadWrite %d num bytes \n", n)
	log.Printf("err:!!!!%s \n", err)
	if err != nil && err != io.EOF {
		log.Printf("HadoopReadWriteGzipFile failed !%s \n", err)
	}
	//force commit write
	CloseGZ(f)
	err = LocalFileReadWriteOssFile(f, h, ossClient, bucket, ossDir, b)
	if err != nil {
		return err
	}
	//every thing is ok, now we need to delete this file
	err = os.Remove(f.f.Name())
	if err != nil {
		log.Printf("Failed to remove file, fileName is %s, errmessage:%s ~ \n", f.f.Name(), err)
	}
	return nil
}

func ReadHdfsWriteForMigrate(hdfsDir string, ossDir string, bucket string,
	hdfsClient *hdfs.Client, ossClient *oss.Client, cm bool) (int, error) {
	if ossDir[len(ossDir)-1:] != "/" {
		return 0, fmt.Errorf("ossDir:%s parameter have to be / in the end!", ossDir)
	}
	if hdfsDir[len(hdfsDir)-1:] != "/" {
		return 0, fmt.Errorf("hdfsDir:%s parameter have to be / in the end!", hdfsDir)
	}
	hadoopFiles, err := listAllDirFiles(hdfsDir, hdfsClient)
	log.Printf("%d num hadoop files should be copy to !", len(hadoopFiles))
	if err != nil {
		return 0, nil
	}
	countNum := 0
	for _, h := range hadoopFiles {
		//if use gzip
		if cm {
			err := gzWriteToOss(h, hdfsClient, ossClient, ossDir, bucket, true)
			if err != nil {
				return 0, err
			}
		} else {
			err := writeToOss(h, ossDir, hdfsClient, ossClient, bucket, true)
			if err != nil {
				return 0, err
			}
		}
		countNum++
	}
	return countNum, nil
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func getDateStamp(hdfsPath string) (string, error) {
	regex := `([a-zA-Z1-9]+)/hourly/(20\d{2})/(\d*)/(\d*)`
	r := regexp.MustCompile(regex)
	matchStr := r.FindStringSubmatch(hdfsPath)
	if len(matchStr) != 5 {
		return "", nil
	} else {
		return matchStr[1] + "/" + matchStr[2] + matchStr[3] + matchStr[4], nil
	}
}

func writeToOss(h HadoopFile, ossDir string, hdfsClient *hdfs.Client,
	ossClient *oss.Client, bucket string, b bool) error {
	log.Printf("Start to sync %s to ossDir %s\n", h.dirPrefix+h.fileInfo.Name(), ossDir)
	hdfsPath := h.dirPrefix + h.fileInfo.Name()
	timeStamp := fmt.Sprintf("%d", makeTimestamp())
	ossPath := ossDir + timeStamp + h.fileInfo.Name()
	if b {
		dateStr, err := getDateStamp(hdfsPath)
		if err != nil {
			log.Printf("get date info from hdfsPath %s failed!", hdfsPath)
			return err
		}
		ossPath = dateStr + "/" + timeStamp + h.fileInfo.Name()
		if err != nil {
			return err
		}
	}
	reader, err := hdfsClient.Open(hdfsPath)
	if err != nil {
		log.Printf("hdfsClient opend failed! err message: %s\n", err)
		return err
	}
	defer reader.Close()
	err = ossClient.PutObject(bucket, ossPath, reader, nil)
	if err != nil {
		log.Printf("Oss Append Object failed %s!\n", err)
		return err
	}
	log.Printf("Finished sync %s to ossDir \n", h.dirPrefix+h.fileInfo.Name(), ossDir)
	return nil
}
