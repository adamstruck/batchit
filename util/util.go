package util

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pkg/errors"
)

func WaitForVolumeStatus(svc *ec2.EC2, volumeId *string, status string) error {
	var xstatus string
	time.Sleep(5 * time.Second)

	for i := 0; i < 30; i++ {
		drsp, err := svc.DescribeVolumes(
			&ec2.DescribeVolumesInput{
				VolumeIds: []*string{volumeId},
			})
		if err != nil {
			return errors.Wrapf(err, "error waiting for volume: %s status: %s", *volumeId, status)
		}
		if len(drsp.Volumes) == 0 {
			panic(fmt.Sprintf("volume: %s not found", *volumeId))
		}
		xstatus = *drsp.Volumes[0].State
		if xstatus == status {
			return nil
		}
		time.Sleep(4 * time.Second)
		if i > 10 {
			time.Sleep(time.Duration(i) * time.Second)
		}
	}
	return fmt.Errorf("never found volume: %s with status: %s. last was: %s", *volumeId, status, xstatus)
}
