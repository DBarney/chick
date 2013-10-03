chick
=====

Generic queueing with dynamic high water marks and preemptive timeouts.

## Why?

Some one is probably asking: "why, oh why are you writting a bottle neck that pretends to be a library!?"
Others are probably asking: "How does introducing a bottleneck and rejecting requests speed up my code?"

You can read more about why oh why i made this library here (short):
http://saasinterrupted.com/2010/02/24/high-availability-principle-request-queueing/

or here (warning lots of gifs):
http://ferd.ca/rtb-where-erlang-blooms.html

or see that Heroku has an implementation of it in thier routers (not code):
https://devcenter.heroku.com/articles/http-routing#request-queueing

## Really Why

I needed a way to:
* queue up code to be run
* fail quickly
* dynamically change the levels of the queue
* divide work over multiple cores
* not spawn 50,000 processes just for individual queue management
* really wanted to say 'chick:in' when talking about code


## Usage

```erlang
%% key can be any term.
key = {1234,<<"some key of some kind">>,[],my_atom}, 
HighWater = 10,
Threshold = 20,
case chick:out(Key,HighWater,Threshold) of
	ok ->
		%% you put your code here

		%% when you are done, you chick it back in
		chick:in(Key);
	{error,Reason} ->
		%% you put your code here
end
```

