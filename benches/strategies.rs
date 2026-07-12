use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rota::{BalanceStrategy, PoolView, TunnelMetrics, RoundRobin, Random, LowestRtt, LeastConnections, HashByAddr, WeightedRoundRobin, Failover, HealthWeighted, Sticky};
use std::time::Duration;

fn make_metrics(rtts: &[Option<u64>], active: &[u32]) -> Vec<TunnelMetrics> {
    rtts.iter()
        .zip(active.iter().chain(std::iter::repeat(&0)))
        .map(|(rtt, &active)| TunnelMetrics {
            rtt: rtt.map(Duration::from_millis),
            active_connections: active,
            ..Default::default()
        })
        .collect()
}

fn bench_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_robin");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = RoundRobin::new();
            let metrics = make_metrics(&vec![Some(10); n], &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("random");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = Random::new();
            let metrics = make_metrics(&vec![Some(10); n], &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_lowest_rtt(c: &mut Criterion) {
    let mut group = c.benchmark_group("lowest_rtt");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = LowestRtt::new();
            let rtts: Vec<Option<u64>> = (0..n).map(|i| Some((i + 1) as u64 * 10)).collect();
            let metrics = make_metrics(&rtts, &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_least_connections(c: &mut Criterion) {
    let mut group = c.benchmark_group("least_connections");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = LeastConnections::new();
            let active: Vec<u32> = (0..n).map(|i| i as u32).collect();
            let metrics = make_metrics(&vec![Some(10); n], &active);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_hash_by_addr(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_by_addr");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = HashByAddr::new();
            let metrics = make_metrics(&vec![Some(10); n], &vec![0; n]);
            let addr = "api.example.com:443";
            let v = PoolView {
                dial_addr: addr,
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_weighted_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("weighted_round_robin");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = WeightedRoundRobin::new();
            let rtts: Vec<Option<u64>> = (0..n).map(|i| Some((i + 1) as u64 * 10)).collect();
            let metrics = make_metrics(&rtts, &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_failover(c: &mut Criterion) {
    let mut group = c.benchmark_group("failover");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = Failover::new();
            let metrics = make_metrics(&vec![Some(10); n], &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            // Initialize the length
            s.pick(&v);
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_health_weighted(c: &mut Criterion) {
    let mut group = c.benchmark_group("health_weighted");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = HealthWeighted::new();
            let rtts: Vec<Option<u64>> = (0..n).map(|i| Some((i + 1) as u64 * 10)).collect();
            let metrics = make_metrics(&rtts, &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

fn bench_sticky(c: &mut Criterion) {
    let mut group = c.benchmark_group("sticky");
    for n in [2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut s = Sticky::new();
            let rtts: Vec<Option<u64>> = (0..n).map(|i| Some((i + 1) as u64 * 10)).collect();
            let metrics = make_metrics(&rtts, &vec![0; n]);
            let v = PoolView {
                dial_addr: "example.com:443",
                metrics: &metrics,
            };
            b.iter(|| black_box(s.pick(&v)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_round_robin,
    bench_random,
    bench_lowest_rtt,
    bench_least_connections,
    bench_hash_by_addr,
    bench_weighted_round_robin,
    bench_failover,
    bench_health_weighted,
    bench_sticky
);
criterion_main!(benches);