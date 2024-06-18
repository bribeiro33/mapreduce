<h1>MapReduce framework</h1>
<h3>Example Run</h3>

```
$ mapreduce-manager --loglevel=DEBUG # Terminal 1
$ mapreduce-worker --port 6001 --loglevel=DEBUG # Terminal 2
$ mapreduce-worker --port 6002 --loglevel=DEBUG # Terminal 3
$ mapreduce-submit --input tests/testdata/input_small # Terminal 1 (manager)

```
<h3>Manager command-line options</h3>
<ul>
  <li>host: host addr</li>
  <li>port: TCP port for messages and UDP port for heartbeats</li>
  <li>logfile: default to stderr if none provided</li>
  <li>loglevel</li>
  <li>shared_dir</li>
</ul>
<h3>Worker command-line options</h3>
<ul>
  <li>host: host addr</li>
  <li>port: TCP port to listen for msgs</li>
  <li>manager-host: addr to send msgs to Manager</li>
  <li>manager-port: Manager's TCP and UDP port to send messages/heartbeat</li>
  <li>logfile: default to stderr if none provided</li>
  <li>loglevel</li>
</ul>
