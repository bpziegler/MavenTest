println("This is from the resource file")

NUM = 10
a = 0
b = 1

println("Loop Size = " + NUM)

start = java.lang.System.currentTimeMillis()
// println("Start = " + start)

for (i = 0; i < NUM; i++) {
	a = a * 1.00001 + 0.72
}

println(a)

elap = java.lang.System.currentTimeMillis() - start
println("Elap = " + elap)
