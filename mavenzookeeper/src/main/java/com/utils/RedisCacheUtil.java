package com.utils;


import org.springframework.stereotype.Service;
import org.thymeleaf.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * redis 工具类
 * @author LHM
 *
 */
@Service
public class RedisCacheUtil {
    private static JedisCluster jedisCluster;

    public RedisCacheUtil(){
        try {
            HostAndPort hostAndPort=new HostAndPort("192.168.194.128",6379);
            jedisCluster=new JedisCluster(hostAndPort);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public  void setJedisCluster(JedisCluster jedisCluster) {
        RedisCacheUtil.jedisCluster = jedisCluster;
    };

    /**
     * 指定String  key  删除
     * @param key
     */
    public static void delete(String key) {
        jedisCluster.del(key);
    }
    /**
     *  取出 缓存 数据
     * @param key
     * @return
     */
    public static String  get(String key) {
        String value = jedisCluster.get(key);
        return value;
    }

    /**
     * 删除 key 存贮
     * @param key
     * @return
     */
    public static Long  del(String key) {
        Long value = jedisCluster.del(key);
        return value;
    }
    /**
     * 设置 过期时间 单位秒
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public static  void setTimeSecond(String key,String value,int seconds ) {
        jedisCluster.setex(key, seconds, value);
    }
    /**
     * 设置 过期时间 单位秒
     */
    public static void setTimeSecond(String key, int seconds){
        try {
            jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 设置 过期时间 单位毫秒
     * @param key
     * @param value
     * @param milliseconds
     * @return
     */
    public static  void setTimeMilliseconds(String key,String value,long milliseconds ) {
        jedisCluster.psetex(key, milliseconds, value);
    }
    /**
     * 设置 过期时间 以天为单位
     * @param key
     * @param value
     * @param day
     * @return
     */
    public static  void setTimeDay(String key,String value,int day ) {
        jedisCluster.psetex(key, day*24*60*60, value);
    }
    /**
     * 设置 过期时间 以小时为单位
     * @param key
     * @param value
     * @param Hour
     * @return
     */
    public static  void setTimeHour(String key,String value,int Hour ) {
        jedisCluster.psetex(key, Hour*60*60, value);
    }
    /**
     * 设置 过期时间 以分钟为单位
     * @param key
     * @param value
     * @param minute
     * @return
     */
    public static  void setTimeMinute(String key,String value,int minute ) {
        jedisCluster.psetex(key, minute*60, value);
        String string = jedisCluster.get(key);
        System.out.println(string);

    }

    /**
     * 自增：计数
     * @param key 已保存的key值
     */
    public static void incr(String key){
        jedisCluster.incr(key);

    }
    /**
     * @Description :  根据key存储map集合
     * @Author : zhangdong
     * @Date : 2018/7/12 11:01
     */
    public  static void hmsetMap(Object key, Map<String, String> hash) {
        try {
            jedisCluster.hmset(key.toString(), hash);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //返还到连接池
            //close(jedis);
        }
    }
    /**
     * @Description : 根据key取出map集合
     * @Author : zhangdong
     * @Date : 2018/7/12 11:02
     * @return : java.util.Map<java.lang.String,java.lang.String>
     */
    public static Map<String,String> hmgetMap(String key) {
        Map<String, String> stringStringMap = null;
        try {
            stringStringMap = jedisCluster.hgetAll(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringStringMap;
    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static byte[] get(byte[] key) {

        byte[] value = null;

        // JedisCluster jedis =null;
        try {
            // jedis = new JedisCluster(jedisClusterNode, config);
            value = jedisCluster.get(key);
        } catch (Exception e) {
            // 释放redis对象
            // close(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            // close(jedis);
        }

        return value;
    }
    /**
     * 存入缓存数据
     * @param key
     * @param
     */
    public static  void set(String key,String value) {
        jedisCluster.set(key, value);
    }
    /**
     * 存入缓存数据
     * @param key
     * @param time 删除时间 单位秒
     */
    public static  void set(String key,String value,int time) {
        jedisCluster.set(key, value);
        jedisCluster.expire(key, time);
    }
    /**
     * 存入缓存数
     * @param key
     * @param value
     */
    public static void set(byte[] key, byte[] value) {

        // JedisCluster jedis =null;
        try {
            // jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.set(key, value);
        } catch (Exception e) {
            // 释放redis对象
            // close(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            // close(jedis);
        }
    }
    /**
     * 存入缓存数据
     * @param key
     * @param value
     * @param time
     */
    public static void set(byte[] key, byte[] value, int time) {
        // JedisCluster jedis =null;
        try {
            // jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.set(key, value);
            jedisCluster.expire(key, time);
        } catch (Exception e) {
            // 释放redis对象
            // close(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            // close(jedis);
        }
    }
    /**
     * redis使用接口
     * @param key
     * @param value
     * @param time
     * @throws Exception
     */
    public static void setRedis(String key, Object value, Integer time) throws Exception {
        if (time != null) {
            set(key.getBytes(), ObjectUtil.objectToBytes(value), time);
        } else {
            set(key.getBytes(), ObjectUtil.objectToBytes(value));
        }
    }
    public static void setRedis(String key, Object value) throws Exception {
        set(key.getBytes(), ObjectUtil.objectToBytes(value));
    }
    public static Object getRedis(String key) throws Exception {
        if (null != RedisCacheUtil.get(key.getBytes())) {
            return ObjectUtil.bytesToObject(RedisCacheUtil.get(key.getBytes()));
        }else{
            return null;
        }
    }

    /**
     * 分布式获取锁
     * @param key 锁关键字
     * @param seconds 锁过期时间
     * @return
     */
    public static boolean  getLock(String key, int seconds){
        return getLock(key,seconds,0);
    }

    /**
     * 分布式获取锁
     * n：次数尝试
     **/
    private static boolean getLock(String key, int seconds,int n){
        try {
            if(n>10){
                TimeUnit.MILLISECONDS.sleep(new Random().nextInt(seconds));
                //throw new MyRuntimeException("获取分布式锁超时");
            }
            long nowtime=System.currentTimeMillis();
            long stopTime=nowtime+seconds*1000L;
            Long flag= jedisCluster.setnx(key,String.valueOf(stopTime));
            if(flag==1){
                jedisCluster.expire(key, seconds);
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(200*seconds);
            return getLock(key,seconds,++n);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 释放锁
     * @param key
     */
    public static void releaseLock(String key){
        Long a= null;
        try {
            String value = get(key);

            //如果获取不到值，则说明该KEY已经过期被删除，因此不用再做释放锁的操作
            if(value != null){
                a=jedisCluster.del(key);
                if (a == 0){
                    TimeUnit.MILLISECONDS.sleep(2000);
                    releaseLock(key);
                }
            }
        }catch (Exception e){
            try {
                TimeUnit.MILLISECONDS.sleep(2000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            releaseLock(key);
        }
    }

    /**
     * 如果redis中不存在指定键值对，则将键值对存到redis
     * @author pangfeichuan
     * @date 2018/6/5 15:48
     * @since 1.0.0
     * @param key KEY
     * @param value VALUE
     * @return <p> 在redis中保存value成功，返回true</p>
     *         <p>如果redis中已经存在指定键值对，则返回false</p>
     */
    public static boolean setIfNotExist(String key,String value){
        String lockKey = "lk_redis" + "setIfNotExist";

        //获取锁的结果
        boolean lockResult = getLock(lockKey,3,2);

        try {
            if(lockResult){
                //根据key从redis获取value
                String expectedValue = get(key);

                //redis中已经存在，则不再往redis中存值，并返回false
                if(!StringUtils.isEmpty(expectedValue)){
                    return false;
                }else{
                    //在redis中保存value成功，返回true（600秒缓存时间）
                    setTimeSecond(key,value,600);
                    return true;
                }
            }else{
                return false;
            }
        }finally {
            //释放锁
            releaseLock(lockKey);
        }
    }



    /**
     * 存储REDIS队列 顺序存储
     * @param  key reids键名
     * @param  value 键值
     */
    public static Long lpush(String key, Object value) {
        return lpush(key,value,null);
    }
    /**
     * 存储REDIS队列 顺序存储
     * @param  key reids键名
     * @param  value 键值
     */
    public static Long lpush(String key, Object value,Integer time) {
        Long len=null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            len=jedisCluster.lpush(ObjectUtil.objectToBytes(key), ObjectUtil.objectToBytes(value));
            if(time!=null) {
                jedisCluster.expire(key, time);
            }
        } catch (Exception e) {

            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            //close(jedis);

        }
        return len;
    }

    /**
     * 存储REDIS队列 反向存储
     * @param  key reids键名
     * @param  value 键值
     */
    public static Long rpush(String key, Object value) {
        Long len=null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            len=  jedisCluster.rpush(ObjectUtil.objectToBytes(key), ObjectUtil.objectToBytes(value));

        } catch (Exception e) {

            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            //close(jedis);

        }
        return len;
    }

    /**
     * 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端
     * @param  key reids键名
     * @param destination 键值
     */
    public static void rpoplpush(String key, Object destination) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.rpoplpush(ObjectUtil.objectToBytes(key), ObjectUtil.objectToBytes(destination));

        } catch (Exception e) {

            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            //close(jedis);

        }
    }

    /**
     * 获取数据
     * @param  key 键名
     * @return
     */
    public static void lpopList(String key, Function<Object,Boolean> f) {
        List<byte[]> list = null;
        try {
            list = jedisCluster.lrange(ObjectUtil.objectToBytes(key), 0, -1);
            if(list==null||list.size()==0) return;
            AtomicBoolean flag=new AtomicBoolean(true);
            list.forEach((b)->{
                try {
                    if(flag.get()){
                        Boolean bflag= f.apply(ObjectUtil.bytesToObject(b
                        ));
                        if(bflag!=null&&!bflag) flag.set(bflag);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    /**
     * 获取队列数据
     * @param  key 键名
     * @return
     */
    public static Object rpop(String key) {
        Object object = null;
        byte[] bytes = null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            bytes = jedisCluster.rpop(ObjectUtil.objectToBytes(key));
            object=ObjectUtil.bytesToObject(bytes);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            //close(jedis);
        }
        return object;
    }

    public static void hmset(Object key, Map<String, String> hash) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hmset(key.toString(), hash);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }
    public static void hmsetKeyMkey(Object key, String mkey,String mValue) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hset(key.toString(),mkey,mValue);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }
    public static void hmdelKeyMkey(Object key, String mkey) {
        try {
            jedisCluster.hdel(key.toString(),mkey);
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }

    public static void hmset(Object key, Map<String, String> hash, int time) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hmset(key.toString(), hash);
            jedisCluster.expire(key.toString(), time);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }
    public static String hget(String key, String mapKey) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            return jedisCluster.hget(key,mapKey);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return null;
    }

    public static List<String> hmget(Object key, String... fields) {
        List<String> result = null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            result = jedisCluster.hmget(key.toString(), fields);

        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return result;
    }

    public static Set<String> hkeys(String key) {
        Set<String> result = null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            result = jedisCluster.hkeys(key);

        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return result;
    }

    public static long llen(byte[] key) {

        long len = 0;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            len= jedisCluster.llen(key);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            //close(jedis);
        }
        return len;

    }
    public static long llenStringKey(String key) {

        long len = 0;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            len= jedisCluster.hlen(key);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            //close(jedis);
        }
        return len;
    }

    /**
     * 获取长度
     * @param key
     * @return
     */
    public static Long llen(String key) {
        try {
            return  llen(ObjectUtil.objectToBytes(key));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /************hset 底层为byte版本*************/

    /**
     * map类型存储   将整个map存储,redis底层为二进制存储
     * @param key
     * @param hash
     */
    public static void hmsetByte(String key, Map<byte[],byte[]> hash) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hmset(ObjectUtil.objectToBytes(key),hash);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }

    /**
     * map类型存储   将制定key的制定map的key的值存入,redis底层为二进制存储
     * @param key
     * @param mKey
     * @param val
     */
    public static void hsetByte(String key, String mKey,Object val) {
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hset(ObjectUtil.objectToBytes(key),ObjectUtil.objectToBytes(mKey),ObjectUtil.objectToBytes(val));
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }

    /**
     * map类型存储   通过key获取map,redis底层为二进制存储
     * @param key
     * @param mKey
     * @return
     */
    public static Object hgetByte(String key, String mKey) {
        Object object=null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            byte[] bytes= jedisCluster.hget(ObjectUtil.objectToBytes(key),ObjectUtil.objectToBytes(mKey));
            if(bytes!=null) object=ObjectUtil.bytesToObject(bytes);
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return object;
    }

    /**
     * 删除map中的key
     * @param key
     * @param mKey
     * @return
     */
    public static Object hdelByte(String key, String mKey) {
        Object object=null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            jedisCluster.hdel(ObjectUtil.objectToBytes(key),ObjectUtil.objectToBytes(mKey));
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return object;
    }
    /**
     * map类型存储   通过key获取全部的map,redis底层为二进制存储,以fun形式向外返回
     * @param key
     * @return
     */
    public static void hgetAllByte(String key,BiFunction<String,Object,Boolean> fun) {
        try {
            Map<byte[],byte[]> bytes= jedisCluster.hgetAll(ObjectUtil.objectToBytes(key));
            if(bytes==null) return;
            AtomicBoolean flag=new AtomicBoolean(true);
            bytes.forEach((k, val)->{
                try {
                    if(flag.get()) {
                        boolean b=fun.apply(ObjectUtil.bytesToObject(k).toString(),ObjectUtil.bytesToObject(val));
                        if(!b) flag.set(b);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
    }
    /**
     * map类型存储   通过key获取map,redis底层为二进制存储
     * @param key
     * @param key
     * @return
     */
    public static Long hlenByte(String key) {
        Object object=null;
        //JedisCluster jedis =null;
        try {
            //jedis = new JedisCluster(jedisClusterNode, config);
            return jedisCluster.hlen(ObjectUtil.objectToBytes(key));
        } catch (Exception e) {
            //释放redis对象
            //close(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            //close(jedis);

        }
        return null;
    }
}
