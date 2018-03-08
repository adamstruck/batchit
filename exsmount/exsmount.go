package exsmount

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	arg "github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/base2genomics/batchit"
	"github.com/base2genomics/batchit/ddv"
	"github.com/base2genomics/batchit/util"
	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().Unix())
}

// IID holds the attributes from the instance identity document
type IID struct {
	AvailabilityZone string `json:"availabilityZone"`
	InstanceId       string `json:"instanceId"`
	InstanceType     string `json:"instanceType"`
	ImageId          string `json:"imageId"`
	Region           string `json:"region"`
}

func (i *IID) Get() error {
	rsp, err := http.Get("http://169.254.169.254/latest/dynamic/instance-identity/document")
	if err != nil {
		return err
	}
	d := json.NewDecoder(rsp.Body)
	return d.Decode(i)
}

type Args struct {
	Size       int64  `arg:"-s,help:size in GB of desired EBS volume"`
	MountPoint string `arg:"-m,required,help:directory on which to mount the EBS volume"`
	VolumeType string `arg:"-v,help:desired volume type; gp2 for General Purpose SSD; io1 for Provisioned IOPS SSD; st1 for Throughput Optimized HDD; sc1 for HDD or Magnetic volumes; standard for infrequent"`
	FSType     string `arg:"-t,help:file system type to create (argument must be accepted by mkfs)"`
	Iops       int64  `arg:"-i,help:Provisioned IOPS. Only valid for volume type io1. Range is 100 to 20000 and <= 50*size of volume."`
	N          int    `arg:"-n,help:number of volumes to request. These will be RAID0'd into a single volume for better write speed and available as a single drive at the specified mount point."`
	Keep       bool   `arg:"-k,help:dont delete the volume(s) on termination (default is to delete)"`
}

func (a Args) Version() string {
	return batchit.Version
}

type LocalArgs struct {
	MountPrefix string   `arg:"positional,required,help:local path to mount devices."`
	Devices     []string `arg:"positional,help:devices to mount. e.g. (/dev/xvd*). Devices that are already mounted will be skipped."`
}

func (l LocalArgs) Version() string {
	return fmt.Sprintf("localmount %s", batchit.Version)
}

func (l LocalArgs) Description() string {
	return "RAID-0, mkfs and mount a series of drives."
}

func mountedDevices() map[string]bool {
	devices := make(map[string]bool)
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return devices
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		dev := strings.Fields(line)[0]
		devices[dev] = true
		for i := len(dev) - 1; i > 1; i-- {
			v := dev[i]
			if '0' <= v && v <= '9' {
				dev = dev[:len(dev)-1]
			} else {
				break
			}
		}
		devices[dev] = true

	}

	return devices
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// MountLocal RAID-0's all devices onto a single mount-point.
func MountLocal(deviceCandidates []string, mountBase string) ([]string, error) {
	inUse := mountedDevices()
	var devices []string
	for _, dev := range deviceCandidates {
		sub := dev[:len(dev)-1]
		// skip xvcd1 when we have xvcd
		if contains(deviceCandidates, sub) {
			continue
		}

		if _, err := os.Stat(dev); err != nil {
			if os.IsNotExist(err) {
				break
			}
			return nil, err
		}
		if _, ok := inUse[dev]; ok {
			continue
		}
		devices = append(devices, dev)
	}
	if len(devices) == 0 {
		log.Printf("localmount: no unused local storage found for %s", deviceCandidates)
		return nil, fmt.Errorf("exsmount: no unused local storage found")
	}
	if _, err := exec.LookPath("mdadm"); err != nil || len(devices) == 1 {
		if len(devices) > 1 {
			log.Println("mdadm not found mounting each device to it's own path")
		}
		for i, dev := range devices {
			log.Printf("making fs for %s", dev)
			if err := mkfs("ext4", dev); err != nil {
				if err == MountedError {
					continue
				}
				log.Println(err)
				return nil, err
			}
			base := mountBase
			log.Printf("mounting %s to %s", dev, base)
			if i > 0 {
				base = fmt.Sprintf("%s_%d", mountBase, i)
			}
			if err = makeAndMount(dev, base); err != nil {
				return nil, err
			}
		}
		return devices, nil
	}
	// RAID0
	var raidDev string
	for i := 0; i < 20; i++ {
		rd := fmt.Sprintf("/dev/md%d", i)
		if _, err := os.Stat(rd); err != nil {
			if os.IsNotExist(err) {
				raidDev = rd
				break
			}
		}
	}
	if raidDev == "" {
		return nil, fmt.Errorf("no available /dev/md path found")
	}

	args := []string{"--create", "--verbose", raidDev, "-R", "--level=stripe", fmt.Sprintf("--raid-devices=%d", len(devices))}
	args = append(args, devices...)
	log.Println("creating RAID0 array with:", strings.Join(append([]string{"mdadm"}, args...), " "))

	cmd := exec.Command("mdadm", args...)
	cmd.Stderr, cmd.Stdout = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	if err := mkfs("ext4", raidDev); err != nil {
		return []string{raidDev}, err
	}
	return []string{raidDev}, makeAndMount(raidDev, mountBase)
}

var MountedError = errors.New("drive is already mounted")

func mkfs(fstype, attachDevice string) error {

	cmd := exec.Command("mkfs", "-t", fstype, attachDevice)
	var b bytes.Buffer
	cmd.Stderr, cmd.Stdout = &b, os.Stderr
	if err := cmd.Run(); err != nil {
		stderr := b.String()
		if strings.Contains(stderr, "is mounted") {
			return MountedError
		}
		os.Stderr.WriteString(stderr)
		return err
	}
	return nil
}

func Create(svc *ec2.EC2, iid *IID, size int64, typ string, iops int64, is ...int) (*ec2.Volume, error) {
	suf := ""
	if len(is) > 0 {
		suf = fmt.Sprintf("-%d", is[0])
	}

	cvi := &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String(iid.AvailabilityZone),
		Size:             aws.Int64(size), //GB
		VolumeType:       aws.String(typ),
		TagSpecifications: []*ec2.TagSpecification{
			{
				ResourceType: aws.String("volume"),
				Tags:         []*ec2.Tag{{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("batchit-%s%s", iid.InstanceId, suf))}},
			},
		},
	}
	if typ == "io1" {
		cvi.Iops = aws.Int64(iops)
	}

	rsp, err := svc.CreateVolume(cvi)
	if err != nil {
		return nil, err
	}
	if err := util.WaitForVolumeStatus(svc, rsp.VolumeId, "available"); err != nil {
		return nil, err
	}
	return rsp, nil
}

type EFSArgs struct {
	MountOptions string `arg:"-o,help:options to send to mount command"`
	EFS          string `arg:"positional,required,help:efs DNS and mount path (e.g.fs-XXXXXX.efs.us-east-1.amazonaws.com:/mnt/efs/)"`
	MountPoint   string `arg:"positional,required,help:local directory on which to mount the EBS volume"`
}

// EFSMain mounts and EFS drive
func EFSMain() {
	cli := &EFSArgs{MountPoint: "/mount/efs/"}
	arg.MustParse(cli)

	if err := EFSMount(cli.EFS, cli.MountPoint, cli.MountOptions); err != nil {
		panic(err)
	}
}

// EFSMount will mount the EFS drive to the requested mount-point.
// the efs argument looks like: fs-XXXXXX.efs.us-east-1.amazonaws.com:/mnt/efs/
func EFSMount(efs string, mountPoint string, mountOpts string) error {
	if err := makeDir(mountPoint); err != nil {
		return err
	}
	opts := "rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2"
	if mountOpts != "" {
		opts += "," + mountOpts
	}
	if !strings.Contains(efs, ":") {
		return fmt.Errorf("EFS string must end with path within the mount e.g. :/")
	}
	// https://docs.aws.amazon.com/efs/latest/ug/mounting-fs-mount-cmd-general.html
	cmd := exec.Command("mount", "-t", "nfs4", "-o", opts, efs, mountPoint)
	cmd.Stderr, cmd.Stdout = os.Stderr, os.Stderr
	return cmd.Run()
}

func Attach(svc *ec2.EC2, iid *IID, volumeId string) (string, error) {
	var attached bool

	defer func() {
		if !attached {
			log.Println("batchit: unsuccessful EBS volume attachment, deleting volume")
			err := ddv.DetachAndDelete(volumeId)
			if err != nil {
				log.Println("batchit: error deleting volume:", err)
			}
		}
	}()

	// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
	// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/volume_limits.html
	// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
	prefix := "/dev/sd"
	letters := "fghijklmnopqrstuvwxyz"
	attachDevice := ""
	// start at a random position
	off := rand.Int63n(int64(len(letters)))
	// retry up to 10 times
	for k := int64(0); k < 10; k++ {
		if off+k > int64(len(letters)-1) {
			off = rand.Int63n(int64(len(letters)))
		}
		off, attachDevice = findNextDevNode(prefix, off+k, letters)

		_, err := svc.AttachVolume(&ec2.AttachVolumeInput{
			InstanceId: aws.String(iid.InstanceId),
			VolumeId:   aws.String(volumeId),
			Device:     aws.String(attachDevice),
		})
		if err != nil {
			// race condition attaching devices from multiple containers to the same host /dev address.
			// so retry with randomish wait time.
			log.Printf("retrying EBS attach because of difficulty getting volume. error was: %+T. %s", err, err)
			if strings.Contains(err.Error(), "is already in use") {
				time.Sleep((time.Duration(1 * (k + rand.Int63n(2*k)))) * time.Second)
				continue
			}
			return "", fmt.Errorf("failed to attach device")
		}

		if err := util.WaitForVolumeStatus(svc, aws.String(volumeId), "in-use"); err != nil {
			return "", err
		}

		if !waitForDevice(attachDevice) {
			return "", fmt.Errorf("error waiting for device to attach")
		}

		attached = true
		break
	}

	if !attached {
		return "", fmt.Errorf("failed to attach device")
	}

	return attachDevice, nil
}

func CreateAttach(cli *Args) ([]string, []string, error) {
	var devices []string
	var volumes []string

	iid := &IID{}
	if err := iid.Get(); err != nil {
		return nil, nil, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating session")
	}
	svc := ec2.New(sess, &aws.Config{Region: aws.String(iid.Region), MaxRetries: aws.Int(3)})

	if cli.VolumeType == "io1" {
		if cli.Iops == 0 {
			cli.Iops = 45 * cli.Size
		}
		if cli.Iops < 100 || cli.Iops > 20000 {
			return nil, nil, fmt.Errorf("ebsmount: Iops must be between 100 and 20000")
		}
		if cli.Iops > 50*cli.Size {
			log.Printf("ebsmount: setting IOPs must be <= 50 times size")
			cli.Iops = 45 * cli.Size
			if cli.Iops > 200000 {
				cli.Iops = 20000
			}
		}
	}

	cli.Size = int64(float64(cli.Size)/float64(cli.N) + 0.5)

	for i := 0; i < cli.N; i++ {
		log.Println("batchit: creating EBS volume:", i)

		var rsp *ec2.Volume
		if rsp, err = Create(svc, iid, cli.Size, cli.VolumeType, cli.Iops, i); err != nil {
			return nil, nil, errors.Wrap(err, "error creating volume")
		}

		device, err := Attach(svc, iid, *rsp.VolumeId)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error attaching volume")
		}

		volumes = append(volumes, *rsp.VolumeId)
		devices = append(devices, device)

		if !cli.Keep {
			if err := DeleteOnTermination(svc, iid.InstanceId, *rsp.VolumeId, device); err != nil {
				return nil, nil, errors.Wrap(err, "error setting delete on termination")
			}
		}
	}

	fmt.Println(strings.Join(volumes, " "))
	if err = makeDir(cli.MountPoint); err != nil {
		return nil, nil, err
	}

	return devices, volumes, nil
}

func DeleteOnTermination(svc *ec2.EC2, instanceId string, volumeId string, attachDevice string) error {
	// set delete on termination
	var ad *string
	ad = &attachDevice
	log.Println("ebsmount: setting to delete on termination")
	moi := &ec2.ModifyInstanceAttributeInput{
		InstanceId: aws.String(instanceId),
		BlockDeviceMappings: []*ec2.InstanceBlockDeviceMappingSpecification{
			{
				// TODO: see if attachDevice is required
				DeviceName: ad,
				Ebs: &ec2.EbsInstanceBlockDeviceSpecification{
					DeleteOnTermination: aws.Bool(true),
					VolumeId:            aws.String(volumeId),
				},
			}},
	}
	_, err := svc.ModifyInstanceAttribute(moi)
	return errors.Wrap(err, "error setting delete on termination")
}

func makeAndMount(attachDevice, mountPoint string) error {
	if err := makeDir(mountPoint); err != nil {
		return err
	}

	opts := []string{"mount", "-o", "noatime", attachDevice, mountPoint}
	cmd := exec.Command("mount", opts[1:]...)
	cmd.Stderr, cmd.Stdout = os.Stderr, os.Stderr
	return cmd.Run()
}

func makeDir(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(path, os.FileMode(0777)); err != nil {
				return err
			}
		} else {
			return err
		}

	}
	return nil
}

func LocalMain() {
	cli := &LocalArgs{MountPrefix: "/mount/local/"}
	arg.MustParse(cli)

	if _, err := MountLocal(cli.Devices, cli.MountPrefix); err != nil {
		panic(err)
	}
}

func Main() {
	cli := &Args{
		Size:       200,
		VolumeType: "gp2",
		FSType:     "ext4",
		N:          1,
	}

	if p := arg.MustParse(cli); cli.VolumeType != "st1" && cli.VolumeType != "gp2" && cli.VolumeType != "sc1" && cli.VolumeType != "io1" && cli.VolumeType != "standard" {
		p.Fail("volume type must be one of st1/gp2/sc1/io1")
	} else if cli.N > 16 || cli.N < 1 {
		p.Fail("number of volumes should be between 1 and 16")
	}

	devices, _, err := CreateAttach(cli)
	if err != nil {
		panic(err)
	}

	if devices, err := MountLocal(devices, cli.MountPoint); err != nil {
		panic(err)
	} else if cli.VolumeType == "st1" || cli.VolumeType == "sc1" {
		// https://aws.amazon.com/blogs/aws/amazon-ebs-update-new-cold-storage-and-throughput-options/
		for _, d := range devices {
			cmd := exec.Command("blockdev", "--setra", "2048", d)
			cmd.Stderr, cmd.Stdout = os.Stderr, os.Stderr
			if err := cmd.Run(); err != nil {
				log.Println("warning: error setting read-ahead", err)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "mounted %d EBS drives to %s\n", len(devices), cli.MountPoint)
}

func findNextDevNode(prefix string, off int64, chars string) (int64, string) {
	for i, s := range chars[off:] {
		if _, err := os.Stat(prefix + string(s)); err == nil {
			continue
		} else if os.IsNotExist(err) {
			return off + int64(i), prefix + string(s)
		}
	}
	panic(fmt.Errorf("no device found with prefix: %s", prefix))
}

func waitForDevice(device string) bool {
	for i := 0; i < 30; i++ {
		if _, err := os.Stat(device); err != nil {
			time.Sleep(1 * time.Second)
		} else {
			return true
		}
	}
	return false
}
