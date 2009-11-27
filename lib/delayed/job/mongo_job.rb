module Delayed

  # A job object that is persisted to the database.
  # Contains the work object as a YAML field.
  class Job
    include MongoMapper::Document

    key :priority,  Integer, :default => 0
    key :attempts,  Integer, :default => 0
    key :handler
    key :last_error
    key :run_at,    Time
    key :locked_at, Time
    key :locked_by
    key :failed_at, Time, :default => nil
    timestamps!

    collection_name = 'delayed_jobs'

    def self.last(opts={})
      super(opts.merge(:order => 'id'))
    end

    # When a worker is exiting, make sure we don't have any locked jobs.
    def self.clear_locks!
      collection.update({:locked_by => worker_name}, {"$set" => {:locked_by => nil, :locked_at => nil}}, :multi => true)
    end

    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(message, backtrace = [], time = nil)
      if self.attempts < MAX_ATTEMPTS
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.last_error   = message + "\n" + backtrace.join("\n")
        self.unlock
        save!
      else
        logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
        destroy_failed_jobs ? destroy : update_attributes(:failed_at => Delayed::Job.db_time_now)
      end
    end

    # Try to run one job. Returns true/false (work done/work failed) or nil if job can't be locked.
    def run_with_lock(max_run_time, worker_name)
      logger.info "* [JOB] aquiring lock on #{name}"
      unless lock_exclusively!(max_run_time, worker_name)
        # We did not get the lock, some other worker process must have
        logger.warn "* [JOB] failed to aquire exclusive lock for #{name}"
        return nil # no work done
      end

      begin
        runtime =  Benchmark.realtime do
          invoke_job # TODO: raise error if takes longer than max_run_time
          logger.info "Also destroying self"
          destroy
        end
        # TODO: warn if runtime > max_run_time ?
        logger.info "* [JOB] #{name} completed after %.4f" % runtime
        return true  # did work
      rescue Exception => e
        reschedule e.message, e.backtrace
        log_exception(e)
        return false  # work failed
      end
    end

    # Add a job to the queue
    def self.enqueue(*args, &block)
      object = block_given? ? EvaledJob.new(&block) : args.shift

      unless object.respond_to?(:perform) || block_given?
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end

      priority = args.first || 0
      run_at   = args[1]

      Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
    end

    # Find a few candidate jobs to run (in case some immediately get locked by others).
    # Return in random order prevent everyone trying to do same head job at once.
    def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)
      time_now = db_time_now

      conditions = {}

      if self.min_priority
        conditions["priority"] ||= {}
        conditions["priority"].merge!("$gte" => min_priority)
      end

      if self.max_priority
        conditions["priority"] ||= {}
        conditions["priority"].merge!("$lte" => max_priority)
      end

      overtime = make_date(time_now - max_run_time.to_i)
      query = "(this.run_at <= #{make_date(time_now)} && (this.locked_at == null || this.locked_at < #{overtime}) || this.locked_by == '#{worker_name}') && this.failed_at == null"

      conditions.merge!("$where" => make_query(query))

      records = collection.find(conditions, {:sort => [['priority', 'descending'], ['run_at', 'ascending']], :limit => limit}).map {|x| new(x)} #, :order => NextTaskOrder, :limit => limit)
      records.sort_by { rand() }
    end

    def self.make_date(date)
      "new Date(#{date.to_f * 1000})"
    end

    def make_date(date)
      self.class.make_date(date)
    end

    def self.make_query(string)
      Mongo::Code.new("function() { return (#{string}); }")
    end

    def make_query(string)
      self.class.make_query(string)
    end

    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def self.reserve_and_run_one_job(max_run_time = MAX_RUN_TIME)

      # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      find_available(5, max_run_time).each do |job|
        t = job.run_with_lock(max_run_time, worker_name)
        return t unless t == nil  # return if we did work (good or bad)
      end

      nil # we didn't do any work, all 5 were not lockable
    end

    # Lock this job for this worker.
    # Returns true if we have the lock, false otherwise.
    def lock_exclusively!(max_run_time, worker = worker_name)
      now = self.class.db_time_now

      affected_rows = if locked_by != worker
        overtime = make_date(now - max_run_time.to_i)
        query = "this._id == '#{id}' && (this.locked_at == null || this.locked_at < #{overtime})"

        conditions = {"$where" => make_query(query)}
        matches = collection.find(conditions).count
        collection.update(conditions, {"$set" => {:locked_at => now, :locked_by => worker}}, :multi => true)
        matches
      else
        conditions = {"_id" => Mongo::ObjectID.from_string(id), "locked_by" => worker} 
        matches = collection.find(conditions).count
        collection.update(conditions, {"$set" => {"locked_at" => now}}, :multi => true)
        matches
      end
      if affected_rows == 1
        self.locked_at    = now
        self.locked_by    = worker
        return true
      else
        return false
      end
    end

  private

    # Get the current time (GMT or local depending on DB)
    # Note: This does not ping the DB to get the time, so all your clients
    # must have syncronized clocks.
    def self.db_time_now
      Time.now.utc
    end

  protected

    def before_save
      self.run_at ||= self.class.db_time_now
    end

  end
end
