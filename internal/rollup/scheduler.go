package rollup

import (
	"context"
	"log"
	"time"
)

type Scheduler struct {
	job *Job
}

func NewScheduler(job *Job) *Scheduler {
	return &Scheduler{job: job}
}

func (s *Scheduler) Run(ctx context.Context) {
	// Run hourly rollups every 5 minutes (catches up quickly)
	hourlyTicker := time.NewTicker(5 * time.Minute)
	// Run daily rollups every hour
	dailyTicker := time.NewTicker(1 * time.Hour)
	defer hourlyTicker.Stop()
	defer dailyTicker.Stop()

	// Run once on startup
	s.runHourly(ctx)
	s.runDaily(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-hourlyTicker.C:
			s.runHourly(ctx)
		case <-dailyTicker.C:
			s.runDaily(ctx)
		}
	}
}

func (s *Scheduler) runHourly(ctx context.Context) {
	if err := s.job.RunHourly(ctx); err != nil {
		log.Printf("hourly rollup error: %v", err)
	}
}

func (s *Scheduler) runDaily(ctx context.Context) {
	if err := s.job.RunDaily(ctx); err != nil {
		log.Printf("daily rollup error: %v", err)
	}
}
