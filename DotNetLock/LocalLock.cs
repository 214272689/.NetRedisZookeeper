using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetLock
{
    public sealed class LocalLock : ILock
    {
        // 锁缓存
        private static readonly ConcurrentDictionary<string, object> _lockCache = new ConcurrentDictionary<string, object>();
        // 记录锁持有者（防止别人释放）
        private static readonly ConcurrentDictionary<string, string> _lockOwnerCache = new ConcurrentDictionary<string, string>();
        // 清理过期锁的间隔（避免内存泄漏）
        private static readonly TimeSpan _cleanupInterval = TimeSpan.FromMinutes(5);
        private static DateTime _lastCleanupTime = DateTime.UtcNow;

        /// <summary>
        /// 获取一个锁(需要自己释放)
        /// </summary>
        public bool LockTake(string key, string value, TimeSpan span)
        {
            EnsureUtil.NotNullAndNotEmpty(key, nameof(key));
            EnsureUtil.NotNullAndNotEmpty(value, nameof(value));

            // 定期清理（防止内存泄漏）
            CleanupStaleLocks();

            // 获取或创建锁对象
            var lockObj = _lockCache.GetOrAdd(key, _ => new object());

            // 尝试加锁
            if (Monitor.TryEnter(lockObj, span))
            {
                _lockOwnerCache[key] = value;
                return true;
            }

            return false;
        }

        /// <summary>
        /// 异步获取锁（同步包装，符合本地锁语义）
        /// </summary>
        public Task<bool> LockTakeAsync(string key, string value, TimeSpan span)
        {
            return Task.FromResult(LockTake(key, value, span));
        }

        /// <summary>
        /// 释放一个锁（必须是持有者才能释放）
        /// </summary>
        public bool LockRelease(string key, string value)
        {
            EnsureUtil.NotNullAndNotEmpty(key, nameof(key));
            EnsureUtil.NotNullAndNotEmpty(value, nameof(value));

            // 不存在锁直接返回
            if (!_lockCache.TryGetValue(key, out var lockObj) || !_lockOwnerCache.TryGetValue(key, out var owner))
                return true;

            // 只有锁的持有者才能释放
            if (owner != value)
                return false;

            try
            {
                // 安全释放
                Monitor.Exit(lockObj);
                _lockOwnerCache.TryRemove(key, out _);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 异步释放锁
        /// </summary>
        public Task<bool> LockReleaseAsync(string key, string value)
        {
            return Task.FromResult(LockRelease(key, value));
        }

        #region 同步执行锁
        public void ExecuteWithLock(string key, string value, TimeSpan span, Action executeAction)
        {
            if (executeAction == null) return;

            if (LockTake(key, value, span))
            {
                try
                {
                    executeAction();
                }
                finally
                {
                    LockRelease(key, value);
                }
            }
        }

        public T ExecuteWithLock<T>(string key, string value, TimeSpan span, Func<T> executeAction, T defaultValue = default)
        {
            if (executeAction == null) return defaultValue;

            if (LockTake(key, value, span))
            {
                try
                {
                    return executeAction();
                }
                finally
                {
                    LockRelease(key, value);
                }
            }

            return defaultValue;
        }
        #endregion

        #region 异步执行锁
        public async Task ExecuteWithLockAsync(string key, string value, TimeSpan span, Func<Task> executeAction)
        {
            if (executeAction == null) return;

            if (await LockTakeAsync(key, value, span))
            {
                try
                {
                    await executeAction();
                }
                finally
                {
                    await LockReleaseAsync(key, value);
                }
            }
        }

        public async Task<T> ExecuteWithLockAsync<T>(string key, string value, TimeSpan span, Func<Task<T>> executeAction, T defaultValue = default)
        {
            if (executeAction == null) return defaultValue;

            if (await LockTakeAsync(key, value, span))
            {
                try
                {
                    return await executeAction();
                }
                finally
                {
                    await LockReleaseAsync(key, value);
                }
            }

            return defaultValue;
        }
        #endregion

        #region 定时清理（防内存泄漏）
        private void CleanupStaleLocks()
        {
            var now = DateTime.UtcNow;
            if (now - _lastCleanupTime < _cleanupInterval)
                return;

            _lastCleanupTime = now;

            // 没有被占用的锁可以安全清理
            foreach (var key in _lockOwnerCache.Keys)
            {
                if (!_lockOwnerCache.ContainsKey(key))
                {
                    _lockCache.TryRemove(key, out _);
                }
            }
        }
        #endregion
    }
}