<?php defined('SYSPATH') or die('No direct script access.');

/*
 * The task daemon. Reads queued items and executes them
 */

class Controller_Daemon extends Controller_CLI {

	public function before()
	{
		parent::before();

		// Setup
		ini_set("max_execution_time", "0");
		ini_set("max_input_time", "0");
		set_time_limit(0);

		// Signal handler
		pcntl_signal(SIGCHLD, array($this, 'sig_handler'));
		pcntl_signal(SIGTERM, array($this, 'sig_handler'));
		declare(ticks = 1);

		// Load config
		$params = $this->request->param();

		// First key is config
		$config = count($params)
			? reset($params)
			: 'default';

		$this->_config = Kohana::config('daemon')->$config;

		if ( empty($this->_config))
		{
			Kohana::$log->add('error', 'Queue. Config not found ("daemon.' . $config . '"). Exiting.');
			echo 'Queue. Config not found ("daemon.' . $config . '"). Exiting.' . PHP_EOL;
			exit;
		}

		$this->_config['pid_path'] = $this->_config['pid_path'] . 'MangoQueue.' . $config . '.pid';
	}

	protected $_config;
	protected $_sigterm;
	protected $_pids = array();

	/*
	 * Run daemon
	 *
	 * php index.php --uri=daemon
	 */
	public function action_index()
	{
		// fork into background
		$pid = pcntl_fork();

		if ( $pid == -1)
		{
			// Error - fork failed
			Kohana::$log->add('error', 'Queue. Initial fork failed');
			exit;
		}
		elseif ( $pid)
		{
			// Fork successful - exit parent (daemon continues in child)
			Kohana::$log->add('debug', 'Queue. Daemon created succesfully at: ' . $pid);
			file_put_contents( $this->_config['pid_path'], $pid);
			exit;
		}
		else
		{
			// Background process - run daemon

			Kohana::$log->add('debug',strtr('Queue. Config :config loaded, max: :max, sleep: :sleep', array(
				':config' => $config,
				':max'    => $this->_config['max'],
				':sleep'  => $this->_config['sleep']
			)));

			// Write the log to ensure no memory issues
			Kohana::$log->write();

			// run daemon
			$this->daemon();
		}
	}

	/*
	 * Exit daemon (if running)
	 *
	 * php index.php --uri=daemon/exit
	 */
	public function action_exit()
	{
		if ( file_exists( $this->_config['pid_path']))
		{
			$pid = file_get_contents($this->_config['pid_path']);

			if ( $pid !== 0)
			{
				Kohana::$log->add('debug','Sending SIGTERM to pid ' . $pid);
				echo 'Sending SIGTERM to pid ' . $pid . PHP_EOL;

				posix_kill($pid, SIGTERM);

				if ( posix_get_last_error() ===0)
				{
					echo "Signal send SIGTERM to pid ".$pid.PHP_EOL;
				}
				else
				{
					echo "An error occured while sending SIGTERM".PHP_EOL;
					unlink($this->_config['pid_path']);
				}
			}
			else
			{
				Kohana::$log->add("debug", "Could not find MangoQueue pid in file :".$this->_config['pid_path']);
				echo "Could not find task_queue pid in file :".$this->_config['pid_path'].PHP_EOL;
			}
		}
		else
		{
			Kohana::$log->add("error", "MangoQueue pid file ".$this->_config['pid_path']." does not exist");
			echo "MangoQueue pid file ".$this->_config['pid_path']." does not exist".PHP_EOL;
		}
	}

	/*
	 * Get daemon & queue status
	 *
	 * php index.php --uri=daemon/status
	 */
	public function action_status()
	{
		$pid = file_exists($this->_config['pid_path'])
			? file_get_contents($this->_config['pid_path'])
			: FALSE;

		echo $pid
			? 'MangoQueue is running at PID: ' . $pid . PHP_EOL
			: 'MangoQueue is NOT running' . PHP_EOL;

		echo 'MangoQueue has ' . Mango::factory('task')->db()->count('tasks') . ' tasks in queue'.PHP_EOL;
	}

	/*
	 * This is the actual daemon process that reads queued items and executes them
	 */
	protected function daemon()
	{
		while ( ! $this->_sigterm)
		{
			// Find first task that is not being executed
			$task = Mango::factory('task')
				->load(1, array('_id' => 1), NULL, array(), array('e' => array('$exists' => FALSE)));

			if ( $task->loaded() && count($this->_pids) < $this->_config['max'])
			{
				// Task found

				// Update task status
				$task->e = TRUE;
				$task->update();

				// Write log to prevent memory issues
				Kohana::$log->write();

				// Fork process to execute task
				$pid = pcntl_fork();

				if ( $pid == -1)
				{
					Kohana::$log->add('error', 'Queue. Could not spawn child task process.');
					exit;
				}
				elseif ( $pid)
				{
					// Parent - add the child's PID to the running list
					$this->_pids[$pid] = time();
				}
				else
				{
					try
					{
						// Child - Execute task
						Request::factory( Route::get( $task->route )->uri( $task->uri->as_array() ) )->execute();
					}
					catch(Exception $e)
					{
						// Task failed - log message
						Kohana::$log->add('error', strtr('Queue. Task failed - route: :route, uri: :uri, msg: :msg', array(
							':route' => $task->route,
							':uri'   => http_build_query($task->uri->as_array()),
							':msg'   => $e->getMessage()
						)));
					}

					// Remove task from queue
					$task->delete();
					exit;
				}
			}
			else
			{
				// No task in queue - sleep
				usleep($this->_config['sleep']);
			}
		}

		// clean up
		$this->clean();
	}

	/*
	 * Performs clean up. Tries (several times if neccesary)
	 * to kill all children
	 */
	protected function clean()
	{
		$tries = 0;

		while ( $tries++ < 5 && count($this->_pids))
		{
			$this->kill_all();
			sleep(1);
		}

		if ( count($this->_pids))
		{
			Kohana::$log('error','Queue. Could not kill all children');
		}

		// Remove PID file
		unlink($this->_config['pid_path']);

		echo 'MangoQueue exited' . PHP_EOL;
	}

	/*
	 * Tries to kill all running children
	 */
	protected function kill_all()
	{
		foreach ($this->_pids as $pid => $time)
		{
			posix_kill($pid, SIGTERM);
			usleep(1000);
		}

		return count($this->_pids) === 0;
	}

	/*
	 * Signal handler. Handles kill & child died signal
	 */
	public function sig_handler($signo)
	{
		switch ($signo)
		{
			case SIGCHLD: 
				// Child died signal
				while( ($pid = pcntl_wait($status, WNOHANG || WUNTRACED)) > 0)
				{
					// remove pid from list
					unset($this->_pids[$pid]);
				}
			break;
			case SIGTERM:
				// Kill signal
				$this->_sigterm = TRUE;
				Kohana::$log->add('debug', 'Queue. Hit a SIGTERM');
			break;
		}
	}
}