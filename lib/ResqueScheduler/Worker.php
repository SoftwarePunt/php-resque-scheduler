<?php

/**
 * ResqueScheduler worker to handle scheduling of delayed tasks.
 *
 * @package		ResqueScheduler
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @copyright	(c) 2012 Chris Boulton
 * @copyright	(c) 2015 Software Punt
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class ResqueScheduler_Worker
{
	const REDIS_KEY_WORKERS = 'delayed_workers';

	const LOG_NONE = 0;
	const LOG_NORMAL = 1;
	const LOG_VERBOSE = 2;

	/**
	 * Current log level of this worker.
	 *
	 * @see LOG_*
	 * @var int
	 */
	public $logLevel = 0;

	/**
	 * Interval to sleep for between checking schedules, in seconds.
	 *
	 * @var int
	 */
	protected $interval = 5;

	/**
	 * @var string The hostname of this worker.
	 */
	private $hostname;

	/**
	 * String uniquely identifying this worker.
	 *
	 * @var string
	 */
	private $id;

	/**
	 * Indicates whether this worker should keep running.
	 * If set to false, this worker will finish execution after finishing the next check.
	 *
	 * @var bool
	 */
	private $alive;

	/**
	 * Return all scheduler workers known to Resque as instantiated instances.
	 *
	 * @return array
	 */
	public static function all($prune = true)
	{
		if ($prune) {
			self::pruneDeadWorkers();
		}

		$workers = Resque::redis()->smembers(self::REDIS_KEY_WORKERS);

		if (!is_array($workers)) {
			$workers = array();
		}

		$instances = array();

		foreach ($workers as $workerId) {
			$instances[] = self::find($workerId);
		}

		return $instances;
	}

	/**
	 * Given a worker ID, check if it is registered/valid.
	 *
	 * @param string $workerId ID of the worker.
	 * @return boolean True if the worker exists, false if not.
	 */
	public static function exists($workerId)
	{
		return (bool)Resque::redis()->sismember(self::REDIS_KEY_WORKERS, $workerId);
	}

	/**
	 * Given a worker ID, find it and return an instantiated worker class for it.
	 *
	 * @param string $workerId The ID of the worker.
	 * @return ResqueScheduler_Worker Instance of the worker. False if the worker does not exist.
	 */
	public static function find($workerId)
	{
		if (!self::exists($workerId) || false === strpos($workerId, ":")) {
			return false;
		}

		list($hostname, $pid) = explode(':', $workerId, 2);

		$worker = new self();
		$worker->setId($workerId);

		return $worker;
	}

	/**
	 * Set the ID of this worker to a given ID string.
	 *
	 * @param string $workerId ID for the worker.
	 */
	public function setId($workerId)
	{
		$this->id = $workerId;
	}

	/**
	 * Look for any workers which should be running on this server and if
	 * they're not, remove them from Redis.
	 *
	 * This is a form of garbage collection to handle cases where the
	 * server may have been killed and the Resque workers did not die gracefully
	 * and therefore leave state information in Redis.
	 */
	public static function pruneDeadWorkers()
	{
		$workerPids = self::workerPids();
		$workers = self::all(false);

		foreach ($workers as $worker) {
			/**
			 * @var $worker ResqueScheduler_Worker
			 */
			if (is_object($worker)) {
				list($host, $pid) = explode(':', (string)$worker, 2);

				if (in_array($pid, $workerPids) || $pid == getmypid()) {
					continue;
				}

				$worker->unregisterWorker();
			}
		}
	}

	/**
	 * Return an array of process IDs for all of the Resque scheduler workers currently
	 * running on this machine.
	 *
	 * @return array Array of resque-scheduler worker process IDs.
	 */
	public static function workerPids()
	{
		$pids = array();

		exec('ps -A -o pid,command | grep [r]esque-scheduler', $cmdOutput);

		foreach ($cmdOutput as $line) {
			list($pids[],) = explode(' ', trim($line), 2);
		}

		return $pids;
	}

	/**
	 * Initializes a new, idle scheduler worker.
	 */
	public function __construct()
	{
		if (function_exists('gethostname')) {
			$this->hostname = gethostname();
		} else {
			$this->hostname = php_uname('n');
		}

		$this->id = $this->hostname . ':' . getmypid();
		$this->alive = true;
	}

	/**
	 * Perform necessary actions to start a worker.
	 */
	private function startup()
	{
		self::pruneDeadWorkers();

		$this->registerSigHandlers();
		$this->registerWorker();
	}

	/**
	 * Register signal handlers that a worker should respond to.
	 *
	 * TERM, INT, QUIT, SIGUSER1, SIGUSER2, SIGCONT: Stop immediately on the next iteration.
	 */
	private function registerSigHandlers()
	{
		if (!function_exists('pcntl_signal')) {
			return;
		}

		declare(ticks = 1);

		pcntl_signal(SIGTERM, array($this, 'stop'));
		pcntl_signal(SIGINT, array($this, 'stop'));
		pcntl_signal(SIGQUIT, array($this, 'stop'));
		pcntl_signal(SIGUSR1, array($this, 'stop'));
		pcntl_signal(SIGUSR2, array($this, 'stop'));
		pcntl_signal(SIGCONT, array($this, 'stop'));

		$this->log('Registered signal handlers');
	}

	/**
	 * Register this worker in Redis.
	 */
	public function registerWorker()
	{
		Resque::redis()->sadd(self::REDIS_KEY_WORKERS, (string)$this);
	}

	/**
	 * Unregister this worker in Redis. (shutdown etc)
	 */
	public function unregisterWorker()
	{
		$id = (string)$this;

		Resque::redis()->srem(self::REDIS_KEY_WORKERS, $id);
	}

	/**
	 * The primary loop for a worker.
	 *
	 * Every $interval (seconds), the scheduled queue will be checked for jobs
	 * that should be pushed to Resque.
	 *
	 * @param int $interval How often to check schedules.
	 */
	public function work($interval = null)
	{
		$this->startup();

		if ($interval !== null) {
			$this->interval = $interval;
		}

		$this->updateProcLine('Starting');

		while ($this->alive) {
			$this->handleDelayedItems();
			$this->sleep();
		}

		$this->updateProcLine('Stopping');

		$this->unregisterWorker();
	}

	/**
	 * Stops the worker safely.
	 */
	public function stop()
	{
		$this->alive = false;
	}

	/**
	 * Handle delayed items for the next scheduled timestamp.
	 *
	 * Searches for any items that are due to be scheduled in Resque
	 * and adds them to the appropriate job queue in Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function handleDelayedItems($timestamp = null)
	{
		while (($timestamp = ResqueScheduler::nextDelayedTimestamp($timestamp)) !== false) {
			$this->updateProcLine('Processing Delayed Items');
			$this->enqueueDelayedItemsForTimestamp($timestamp);
		}
	}

	/**
	 * Schedule all of the delayed jobs for a given timestamp.
	 *
	 * Searches for all items for a given timestamp, pulls them off the list of
	 * delayed jobs and pushes them across to Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function enqueueDelayedItemsForTimestamp($timestamp)
	{
		$item = null;
		while ($item = ResqueScheduler::nextItemForTimestamp($timestamp)) {
			$this->log('queueing ' . $item['class'] . ' in ' . $item['queue'] . ' [delayed]');

			Resque_Event::trigger('beforeDelayedEnqueue', array('queue' => $item['queue'], 'class' => $item['class'], 'args' => $item['args'],));

			$payload = array_merge(array($item['queue'], $item['class']), $item['args']);
			call_user_func_array('Resque::enqueue', $payload);
		}
	}

	/**
	 * Sleep for the defined interval.
	 */
	protected function sleep()
	{
		sleep($this->interval);
	}

	/**
	 * Update the status of the current worker process.
	 *
	 * On supported systems (with the PECL proctitle module installed), update
	 * the name of the currently running process to indicate the current state
	 * of a worker.
	 *
	 * @param string $status The updated process title.
	 */
	private function updateProcLine($status)
	{
		if (function_exists('setproctitle')) {
			setproctitle('resque-scheduler-' . ResqueScheduler::VERSION . ': ' . $status);
		}
	}

	/**
	 * Output a given log message to STDOUT.
	 *
	 * @param string $message Message to output.
	 */
	public function log($message)
	{
		if ($this->logLevel == self::LOG_NORMAL) {
			fwrite(STDOUT, "*** " . $message . "\n");
		} else if ($this->logLevel == self::LOG_VERBOSE) {
			fwrite(STDOUT, "** [" . strftime('%T %Y-%m-%d') . "] " . $message . "\n");
		}
	}

	/**
	 * Generate a string representation of this worker.
	 *
	 * @return string String identifier for this worker instance.
	 */
	public function __toString()
	{
		return $this->id;
	}
}