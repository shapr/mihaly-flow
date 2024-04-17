extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;
// use timely::PartialOrder;

fn main() {
    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {
	let args: Vec<String> = std::env::args().collect();
	let size: usize = args
	    .get(1)
	    .map(|s| s.parse().expect("Failed to parse size from command line"))
	    .unwrap_or(100);
	// create an input collection of data.
	let mut input = InputSession::new();

	// define a new computation.
	// create a manager
	let probe = worker.dataflow(|scope| {
	    // create a new collection from our input.
	    let manages = input.to_collection(scope);

	    // if (m2, m1) and (m1, p), then output (m1, (m2, p))
	    manages.map(|(m2, m1)| (m1, m2)).join(&manages).probe()
	    // .Inspect| println!("{:?}", x));
	});

	// Load input (a binary tree).
	// input.advance_to(0);
	let mut person = worker.index();
	while person < size {
	    input.insert((person / 2, person));
	    person += worker.peers();
	}

	input.advance_to(1);
	input.flush();
	while probe.less_than(&input.time()) {
	    worker.step();
	}
	println!("{:?}\tdata loaded", worker.timer().elapsed());

	// change its input
	let mut person = 1 + worker.index();
	while person < size {
	    input.remove((person / 2, person));
	    input.insert((person / 3, person));
	    input.advance_to(person);
	    input.flush();
	    while probe.less_than(&input.time()) {
		worker.step();
	    }
	    println!("{:?}\tstep {} complete", worker.timer().elapsed(), person);
	    person += worker.peers();
	}
    })
    .expect("Computation terminated abnormally");
}
