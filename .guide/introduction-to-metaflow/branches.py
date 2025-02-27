from metaflow import FlowSpec, step


class Branches(FlowSpec):
    """A flow that showcases how branches work."""

    @step
    def start(self):
        """Initialize the start value artifact."""
        self.start_value = 0
        
        # NOTE: you trigger a static branch by calling `next` with multiple steps
        self.next(self.step1, self.step2)

    @step
    def step1(self):
        """Assign a value to an artifact."""
        print("Executing Step 1")
        self.common = 1
        self.next(self.join)

    @step
    def step2(self):
        """Assign a value to an artifact."""
        print("Executing Step 2")
        self.common = 2
        self.next(self.join)

    # All branches must be joined at some point!!
    # The join method (which does not need to be named "join") takes in another parameter
    # `inputs` which is a list of all the steps that are being joined.
    @step
    def join(self, inputs):
        """Join the two branches."""
        # Join the steps
        # NOTE: merge_artifacts can be used to control the flow of data artifacts between steps - see https://docs.metaflow.org/metaflow/basics#data-flow-through-the-graph
        self.merge_artifacts(
            inputs, 
            exclude=["common"]  # specify which data artifacts to exclude from the merge -> dont merge common
        )

        # You can refer to each step in the branch by the step name
        print("Step 1's artifact value:", inputs.step1.common)
        print("Step 2's artifact value:", inputs.step2.common)

        # You can also iterate over all the steps in the branch
        self.final_value = sum(i.common for i in inputs)
        self.next(self.end)

    @step
    def end(self):
        """Print the final artifact values."""
        print("Start value:", self.start_value)
        print("Final value:", self.final_value)


if __name__ == "__main__":
    Branches()
