<?php
namespace Stunti\Cache\Backend;

/**
 * @see Zend_Cache_Backend
 */

/**
 * @see Zend_Cache_Backend_ExtendedInterface
 */
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;

/**
 * @author       Olivier Bregeras (Stunti) (olivier.bregeras@gmail.com)
 * @category     Stunti
 * @package      Stunti_Cache
 * @subpackage   Stunti_Cache_Backend
 * @copyright    Copyright (c) 2009 Stunti. (http://www.stunti.org)
 * @license      http://stunti.org/license/new-bsd     New BSD License
 */
class MongoDB extends \Zend_Cache_Backend implements \Zend_Cache_Backend_ExtendedInterface {

	const DEFAULT_HOST       = '127.0.0.1';
	const DEFAULT_PORT       = 27017;
	const DEFAULT_PERSISTENT = false;
	const DEFAULT_DBNAME     = 'Db_Cache';
	const DEFAULT_COLLECTION = 'C_Cache';
	const DEFAULT_SLAVE_OK   = false;
	const DEFAULT_REPLICASET = false;

	/**
	 * @var Client
	 */
	protected $client;
	/**
	 * @var Database
	 */
	protected $db;
	/**
	 * @var Collection
	 */
	protected $collection;

	/**
	 * Available options
	 *
	 * =====> (array) servers :
	 * an array of mongodb server ; each mongodb server is described by an associative array :
	 * 'host' => (string) : the name of the mongodb server
	 * 'port' => (int) : the port of the mongodb server
	 * 'persistent' => (bool) : use or not persistent connections to this mongodb server
	 * 'collection' => (string) : name of the collection to use
	 * 'dbname' => (string) : name of the database to use
	 *
	 * @var array available options
	 */
	protected $options = array(
		'host'       => self::DEFAULT_HOST,
		'port'       => self::DEFAULT_PORT,
		'persistent' => self::DEFAULT_PERSISTENT,
		'collection' => self::DEFAULT_COLLECTION,
		'dbname'     => self::DEFAULT_DBNAME,
		'replicaSet' => self::DEFAULT_REPLICASET,
		'slaveOK'    => self::DEFAULT_SLAVE_OK
	);

	/**
	 * @return void
	 */
	public function __construct($options) {
		if(!extension_loaded('MongoDB')) {
			\Zend_Cache::throwException('The MongoDB extension must be loaded for using this backend !');
		}
		parent::__construct($options);

		// Merge the options passed in; overridding any default options
		$this->options = array_merge($this->options, $options);
	}

	/**
	 * Expires a record (mostly used for testing purposes)
	 *
	 * @param string $id
	 *
	 * @return void
	 */
	public function ___expire($id) {
		$dataFromMongoDB = $this->get($id);
		if($dataFromMongoDB != null) {
			$dataFromMongoDB['l'] = -10;
			$this->collection->save($dataFromMongoDB);
		}
	}

	/**
	 * Test if a cache is available for the given id and (if yes) return it (false else)
	 *
	 * @param  string  $id                     Cache id
	 * @param  boolean $doNotTestCacheValidity If set to true, the cache validity won't be tested
	 *
	 * @return string|false cached datas
	 */
	public function load($id, $doNotTestCacheValidity = false) {
		$dataFromMongoDB = $this->get($id);
		if($dataFromMongoDB != null) {
			if($doNotTestCacheValidity || !$doNotTestCacheValidity && ($dataFromMongoDB['created_at'] + $dataFromMongoDB['l']) >= time()) {
				return $dataFromMongoDB['d'];

				return false;
			}

			return false;
		}
	}

	/**
	 * Test if a cache is available or not (for the given id)
	 *
	 * @param  string $id Cache id
	 *
	 * @return mixed|false (a cache is not available) or "last modified" timestamp (int) of the available cache record
	 */
	public function test($id) {
		$dataFromMongoDB = $this->get($id);
		if($dataFromMongoDB != null) {
			return $dataFromMongoDB['created_at'];
		}

		return false;
	}

	/**
	 * Save some string datas into a cache record
	 *
	 * Note : $data is always "string" (serialization is done by the
	 * core not by the backend)
	 *
	 * @param  string $data             Datas to cache
	 * @param  string $id               Cache id
	 * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
	 * @param  int    $specificLifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
	 *
	 * @return boolean True if no problem
	 */
	public function save($data, $id, $tags = array(), $specificLifetime = false) {
		$lifetime = $this->getLifetime($specificLifetime);
		$flag = 0;

		// #ZF-5702 : we try add() first becase set() seems to be slower

		$result = $this->set($id, $data, $lifetime, $tags);

		return $result;
	}

	/**
	 * Remove a cache record
	 *
	 * @param  string $id Cache id
	 *
	 * @return boolean True if no problem
	 */
	public function remove($id) {
		$this->lazyInitializeTheConnection();

		return $this->collection->deleteOne(array('_id' => $id));
	}

	/**
	 * Clean some cache records (protected method used for recursive stuff)
	 *
	 * Available modes are :
	 * Zend_Cache::CLEANING_MODE_ALL (default)    => remove all cache entries ($tags is not used)
	 * Zend_Cache::CLEANING_MODE_OLD              => remove too old cache entries ($tags is not used)
	 * Zend_Cache::CLEANING_MODE_MATCHING_TAG     => remove cache entries matching all given tags
	 *                                               ($tags can be an array of strings or a single string)
	 * Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG => remove cache entries not {matching one of the given tags}
	 *                                               ($tags can be an array of strings or a single string)
	 * Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG => remove cache entries matching any given tags
	 *                                               ($tags can be an array of strings or a single string)
	 *
	 * @param  string $dir  Directory to clean
	 * @param  string $mode Clean mode
	 * @param  array  $tags Array of tags
	 *
	 * @throws Zend_Cache_Exception
	 * @return boolean True if no problem
	 */
	public function clean($mode = \Zend_Cache::CLEANING_MODE_ALL, $tags = array()) {
		$this->lazyInitializeTheConnection();
		switch($mode) {
			case \Zend_Cache::CLEANING_MODE_ALL:
				return $this->collection->deleteMany(array());
				break;
			case \Zend_Cache::CLEANING_MODE_OLD:
				//$res = $this->_instance->findOneCond(array('$where' => new \MongoCode('function() { return (this.l + this.created_at) < '.(time()-1).'; }')));
				//var_dump($res);exit;
				return $this->collection->deleteMany(array('$where' => new \MongoCode('function() { return (this.l + this.created_at) < ' . (time() - 1) . '; }')));
				break;
			case \Zend_Cache::CLEANING_MODE_MATCHING_TAG:
				return $this->collection->deleteMany(array('t' => array('$all' => $tags)));
				break;
			case \Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
				return $this->collection->deleteMany(array('t' => array('$nin' => $tags)));
				break;
			case \Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
				//find all tags and remove them
				//$this->_log(self::TAGS_UNSUPPORTED_BY_CLEAN_OF_MEMCACHED_BACKEND);
				return $this->collection->deleteMany(array('t' => array('$in' => $tags)));
				break;
			default:
				\Zend_Cache::throwException('Invalid mode for clean() method');
				break;
		}
	}

	/**
	 * Return true if the automatic cleaning is available for the backend
	 *
	 * @return boolean
	 */
	public function isAutomaticCleaningAvailable() {
		return false;
	}

	/**
	 * Set the frontend directives
	 *
	 * @param  array $directives Assoc of directives
	 *
	 * @throws Zend_Cache_Exception
	 * @return void
	 */
	public function setDirectives($directives) {
		parent::setDirectives($directives);
		$lifetime = $this->getLifetime(false);
		if($lifetime === null) {
			// #ZF-4614 : we tranform null to zero to get the maximal lifetime
			parent::setDirectives(array('lifetime' => 0));
		}
	}

	/**
	 * Return an array of stored cache ids
	 *
	 * @return array array of stored cache ids (string)
	 */
	public function getIds() {
		$this->lazyInitializeTheConnection();
		$cursor = $this->collection->find(
			array(),
			array(
				'typeMap' => array(
					'root'     => 'array',
					'document' => 'array',
					'array'    => 'array'
				)
			)
		);
		if($cursor != null) {
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();

			$ret = array();
			while($tmp = $cursor->current()) {
				$ret[] = $tmp['_id'];
				$iterator->next();
			}

			return $ret;
		}
	}

	/**
	 * Return an array of stored tags
	 *
	 * @return array array of stored tags (string)
	 */
	public function getTags() {
		//might have to use map reduce for that (example on Mongodb doc)
		$this->lazyInitializeTheConnection();

		$cmd['mapreduce'] = $this->options['collection'];
		//$cmd['verbose'] = true;
		$cmd['map'] = 'function(){
                                this.t.forEach(
                                    function(z){
                                        emit( z , { count : 1 } );
                                    }
                                );
                            };';
		$cmd['reduce'] = 'function( key , values ){
                                var total = 0;
                                for ( var i=0; i<values.length; i++ )
                                    total += values[i].count;
                                return { count : total };
                            };
            ';

		$res2 = $this->db->command($cmd);
		$res3 = $this->db->selectCollection($res2['result'])->find(
			array(),
			array(
				'typeMap' => array(
					'root'     => 'array',
					'document' => 'array',
					'array'    => 'array'
				)
			)
		);

		$res = array();
		foreach($res3 as $key => $val) {
			$res[] = $key;
		}

		$this->db->dropCollection($res2['result']);

		return $res;
	}

	public function drop() {
		$this->lazyInitializeTheConnection();

		return $this->collection->drop();
	}

	/**
	 * Return an array of stored cache ids which match given tags
	 *
	 * In case of multiple tags, a logical AND is made between tags
	 *
	 * @param array $tags array of tags
	 *
	 * @return array array of matching cache ids (string)
	 */
	public function getIdsMatchingTags($tags = array()) {
		$this->lazyInitializeTheConnection();
		$cursor = $this->collection->find(
			array('t' => array('$all' => $tags)),
			array(
				'typeMap' => array(
					'root'     => 'array',
					'document' => 'array',
					'array'    => 'array'
				)
			)
		);
		if($cursor != null) {
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();
			$ret = array();
			while($tmp = $iterator->current()) {
				$ret[] = $tmp['_id'];

				$iterator->next();
			}

			return $ret;
		}
	}

	/**
	 * Return an array of stored cache ids which don't match given tags
	 *
	 * In case of multiple tags, a logical OR is made between tags
	 *
	 * @param array $tags array of tags
	 *
	 * @return array array of not matching cache ids (string)
	 */
	public function getIdsNotMatchingTags($tags = array()) {
		$this->lazyInitializeTheConnection();
		$cursor = $this->collection->find(
			array('t' => array('$nin' => $tags)),
			array(
				'typeMap' => array(
					'root'     => 'array',
					'document' => 'array',
					'array'    => 'array'
				)
			)
		);
		if($cursor != null) {
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();

			$ret = array();

			while($tmp = $iterator->current()) {
				$ret[] = $tmp['_id'];
				$iterator->next();
			}

			return $ret;
		}
	}

	/**
	 * Return an array of stored cache ids which match any given tags
	 *
	 * In case of multiple tags, a logical AND is made between tags
	 *
	 * @param array $tags array of tags
	 *
	 * @return array array of any matching cache ids (string)
	 */
	public function getIdsMatchingAnyTags($tags = array()) {
		$this->lazyInitializeTheConnection();
		$cursor = $this->collection->find(
			array('t' => array('$in' => $tags)),
			array(
				'typeMap' => array(
					'root'     => 'array',
					'document' => 'array',
					'array'    => 'array'
				)
			)
		);
		if($cursor != null) {
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();

			$ret = array();
			while($tmp = $iterator->current()) {
				$ret[] = $tmp['_id'];
				$iterator->next();
			}

			return $ret;
		}
	}

	/**
	 * No way to find the remaining space right now. So retrun 0.
	 *
	 * @throws Zend_Cache_Exception
	 * @return int integer between 0 and 100
	 */
	public function getFillingPercentage() {
		return 1;
	}

	/**
	 * Return an array of metadatas for the given cache id
	 *
	 * The array must include these keys :
	 * - expire : the expire timestamp
	 * - tags : a string array of tags
	 * - mtime : timestamp of last modification time
	 *
	 * @param string $id cache id
	 *
	 * @return array array of metadatas (false if the cache id is not found)
	 */
	public function getMetadatas($id) {
		$dataFromMongoDB = $this->get($id);
		if($dataFromMongoDB != null) {
			$mtime = $dataFromMongoDB['created_at'];
			$lifetime = $dataFromMongoDB['l'];

			return array(
				'expire' => $mtime + $lifetime,
				'tags'   => $dataFromMongoDB['t'],
				'mtime'  => $mtime
			);
		}

		return false;
	}

	/**
	 * Give (if possible) an extra lifetime to the given cache id
	 *
	 * @param string $id cache id
	 * @param int    $extraLifetime
	 *
	 * @return boolean true if ok
	 */
	public function touch($id, $extraLifetime) {
		$dataFromMongoDB = $this->get($id);
		if($dataFromMongoDB != null) {
			$data = $dataFromMongoDB['d'];
			$mtime = $dataFromMongoDB['created_at'];
			$lifetime = $dataFromMongoDB['l'];
			$tags = $dataFromMongoDB['t'];
			$newLifetime = $lifetime - (time() - $mtime) + $extraLifetime;
			if($newLifetime <= 0) {
				return false;
			}

			// #ZF-5702 : we try replace() first becase set() seems to be slower
			$result = $this->set($id, $data, $newLifetime, $tags);

			return $result;
		}

		return false;
	}

	/**
	 * Return an associative array of capabilities (booleans) of the backend
	 *
	 * The array must include these keys :
	 * - automatic_cleaning (is automating cleaning necessary)
	 * - tags (are tags supported)
	 * - expired_read (is it possible to read expired cache records
	 *                 (for doNotTestCacheValidity option for example))
	 * - priority does the backend deal with priority when saving
	 * - infinite_lifetime (is infinite lifetime can work with this backend)
	 * - get_list (is it possible to get the list of cache ids and the complete list of tags)
	 *
	 * @return array associative of with capabilities
	 */
	public function getCapabilities() {
		return array(
			'automatic_cleaning' => true,
			'tags'               => true,
			'expired_read'       => true,
			'priority'           => false,
			'infinite_lifetime'  => true,
			'get_list'           => true
		);
	}

	/**
	 * @param int   $id
	 * @param array $data
	 * @param int   $lifetime
	 * @param mixed $tags
	 *
	 * @return boolean
	 */
	function set($id, $data, $lifetime, $tags) {
		$this->lazyInitializeTheConnection();
		try {

			$success = $this->collection->updateOne(
				array('_id' => $id),
				array(
					'$set' => array(
						'd'          => $data,
						'created_at' => time(),
						'l'          => $lifetime,
						't'          => $tags
					)
				),
				array(
					'upsert' => true
				)
			);

			// create an index on 'x' ascending
			$this->collection->createIndex(array('t' => 1));
			$this->collection->createIndex(array('created_at' => 1));
		} catch(\Exception $e) {
			throw $e;
		}

		return $success;
	}

	/**
	 * @param int $id
	 *
	 * @return array|false
	 */
	function get($id) {
		$this->lazyInitializeTheConnection();

		$data = $this->collection->findOne(
			array('_id' => $id)
		);

		return $data;
	}

	public function lazyInitializeTheConnection() {
		if($this->client == null) {
			try {
				if($this->options['replicaSet'] != false) {
					$this->client = new Client($this->options['host'], array('replicaSet' => $this->options['replicaSet']));
				} else {
					$this->client = new Client($this->options['host']);
				}

				$this->db = $this->client->selectDatabase(
					$this->options['dbname'],
					array(
						'readPreference' => new ReadPreference(ReadPreference::RP_PRIMARY),
						'writeConcern '  => new WriteConcern(WriteConcern::MAJORITY)
					)
				);
				$this->collection = $this->db->selectCollection($this->options['collection']);
			} catch(\Exception $e) {
				throw $e;
			}
		}
	}
}