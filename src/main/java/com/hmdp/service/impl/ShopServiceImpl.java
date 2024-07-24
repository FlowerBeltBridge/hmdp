package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    //线程池
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    //逻辑过期
//    public Shop queryWithLogicalExpire(Long id) {
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        if (StrUtil.isBlank(shopJson)) {
//            //从redis查询如果为空，返回null
//            return null;
//        }
//        //命中，需要先把Json序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //未过期，直接返回店铺信息
//            return shop;
//        }
//        //过期，开始缓存重建
//        String lockKey = LOCK_SHOP_KEY + id;
//        //获取互斥锁
//        boolean isLock = tryLock(lockKey);
//        //判断是否获取锁成功
//        if (isLock) {
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                //成功，开启独立线程，实现缓存重建
//                try {
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//            });
//
//        }
//        //失败，返回过期店铺信息
//        //返回数据
//        return shop;
//    }

    /**
     * 互斥锁
     * @param id
     * @return
     */
//    public Shop queryWithMutex(Long id) {
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        if (StrUtil.isNotBlank(shopJson)) {
//            //存在返回数据
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//        //获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//            if (!isLock) {
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            //拿到互斥锁，查数据库
//            shop = getById(id);
//            //数据库不存在，返回错误
//            if (shop == null) {
//                //将空值写入Redis
//                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            //数据库存在，写入redis
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            //释放互斥锁
//            unlock(lockKey);
//        }
//
//        //返回数据
//        return shop;
//    }

    /**
     * 缓存穿透
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        //从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)) {
            //存在返回数据
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中是否是空值
        if (shopJson != null) {
            return null;
        }
        //不存在，查数据库
        Shop shop = getById(id);
        //数据库不存在，返回错误
        if (shop == null) {
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //数据库存在，写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回数据
        return shop;
    }

//    /**
//     * 获取互斥锁
//     * @param key
//     * @return
//     */
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }

    /**
     * 重建缓存
     * @param id
     * @param expireSeconds
     */
    public void saveShop2Redis(Long id, Long expireSeconds) {
        //查询店铺
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
