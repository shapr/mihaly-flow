use differential_dataflow::{self, input::Input, operators::CountTotal};
use mihaly_flow::lib::print_hello_world;
use rand::{self, rngs::StdRng, Rng, SeedableRng};
use timely;

fn main() {
    let mut args = std::env::args();
    args.next();
    let nodes: u32 = args.next().unwrap().parse().unwrap();
    let edges: usize = args.next().unwrap().parse().unwrap();
    let batch: u32 = args.next().unwrap().parse().unwrap();
    let inspect: bool = args.next().unwrap() == "inspect";

    timely::execute_from_args(std::env::args().skip(5), move |worker| {
	let timer = ::std::time::Instant::now();
	let index = worker.index();
	let peers = worker.peers();
	// create a degree counting differential dataflow
	let (mut input, probe) = worker.dataflow::<u32, _, _>(|scope| {
	    //create edge input, count a few ways
	    let (input, edges) = scope.new_collection::<_, i32>();
	    let out_degr_distr = edges
		.map(|(src, _dst)| src)
		.count_total()
		.map(|(_src, cnt)| cnt as usize)
		.count_total();
	    let probe = out_degr_distr
		.filter(move |_| inspect)
		.inspect(|x| println!("observed: {:?}", x))
		.probe();
	    (input, probe)
	});

	let seed: &[_] = &[1, 2, 3, index];
	let mut rng1: StdRng = SeedableRng::from_seed(seed);
	let mut rng2: StdRng = SeedableRng::from_seed(seed);

	// load up graph data, round robin among workers
	for _ in 0..(edges / peers) + if index < (edges % peers) { 1 } else { 0 } {
	    input.update((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1)
	}
	input.advance_to(1);
	input.flush();
	worker.step_while(|| probe.less_than(input.time()));
	if index == 0 {
	    println!("round 0 finished after {:?} (loading)", timer.elapsed());
	}
	if batch > 0 {
	    // have worker zero drive input production
	    if index == 0 {
		let mut next = batch;
		for round in 1.. {
		    input.advance_to(round);
		    input.update((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1);
		    input.update((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), -1);

		    if round > next {
			let timer = ::std::time::Instant::now();
			input.flush();
			while probe.less_than(input.time()) {
			    worker.step();
			}
			println!("round {} finished after {:?}", next, timer.elapsed());
			next += batch;
		    }
		}
	    }
	}
    })
    .unwrap();

    print_hello_world();
}
