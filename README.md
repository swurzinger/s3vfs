[![Build Status](https://dev.azure.com/swurzinger/s3vfs/_apis/build/status/swurzinger.s3vfs?branchName=master)](https://dev.azure.com/swurzinger/s3vfs/_build/latest?definitionId=1&branchName=master)
![status: inactive](https://img.shields.io/badge/status-inactive-red.svg)

# This is currently unmaintained
### .. and still has some bugs, so not recommended to be used as-is.
### You can use [rclone](https://github.com/rclone/rclone) instead with the [`rclone mount`](https://rclone.org/commands/rclone_mount/) command.

.

---

.


# S3 virtual file system

Creates a virtual drive, which allows to access S3 objects in a file system structure just as if the data would be stored on a local or network drive.

## Getting Started
 
### Prerequisites
The implementation requires the WinFSP user-mode file system driver to be installed on your system.
 - [WinFSP driver](https://github.com/billziss-gh/winfsp/releases)
 - [.NET 5.0 runtime](https://dotnet.microsoft.com/download) (if not using the standalone version)

### Mounting S3 buckets as drive
Run the following command (filling in your S3 credentials) to mount all accessible buckets as drive `Z:`
```
s3vfs -e "s3.amazonaws.com" -a "YOUR-ACCESSKEYID" -s "YOUR-SECRETACCESSKEY" -m Z:
```

### Unmount
To unmount the drive simply stop the application by pressing <kbd>Ctrl</kbd>+<kbd>C</kbd>

## Usage
```
  -e, --s3Url <s3Url>                S3 service endpoint url
  -a, --accessKey <accessKey>        S3 access key
  -s, --secretKey <secretKey>        S3 secret key
  -u, --volumePrefix <volumePrefix>  Volume Prefix (e.g. \prefix\service)
  -m, --mountPoint <mountPoint>      MountPoint (e.g. Z:)
  --version                          Show version information
  -?, -h, --help                     Show help and usage information
```

## Compatibility
 - Microsoft Windows 7 or higher (as required by WinFSP)
 - Amazon AWS, MinIO or any other compatible S3 service should work

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
