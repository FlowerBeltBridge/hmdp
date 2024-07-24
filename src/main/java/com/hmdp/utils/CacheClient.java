package com.hmdp.utils;

import cn.hutool.core.lang.func.Func;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            //存在返回数据
            return JSONUtil.toBean(json, type);
        }
        //判断命中是否是空值
        if (json != null) {
            return null;
        }
        //不存在，查数据库
        R r = dbFallback.apply(id);
        //数据库不存在，返回错误
        if (r == null) {
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //数据库存在，写入redis
        this.set(key, r, time, unit);
        //返回数据
        return r;
    }

    /**
     * 线程池
     **/
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 缓存击穿，逻辑过期
     **/
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(    key);
        if (StrUtil.isBlank(shopJson)) {
            //从redis查询如果为空，返回null
            return null;
        }
        //命中，需要先把Json序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期，直接返回店铺信息
            return r;
        }
        //过期，开始缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        //获取互斥锁
        boolean isLock = tryLock(lockKey);
        //判断是否获取锁成功
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //成功，开启独立线程，实现缓存重建
                try {
                    //查数据库
                    R r1 = dbFallback.apply(id);
                    //写入Redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }
        //失败，返回过期店铺信息
        //返回数据
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
