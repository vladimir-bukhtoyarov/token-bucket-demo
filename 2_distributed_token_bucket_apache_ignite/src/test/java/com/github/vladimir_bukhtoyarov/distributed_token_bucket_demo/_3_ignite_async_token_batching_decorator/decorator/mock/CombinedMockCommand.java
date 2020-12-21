package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator.mock;

import java.util.List;
import java.util.stream.Collectors;

public class CombinedMockCommand implements MockCommand<CombinedResult> {

    private final List<MockCommand> commands;

    public CombinedMockCommand(List<MockCommand> commands) {
        this.commands = commands;
    }

    public List<MockCommand> getCommands() {
        return commands;
    }

    @Override
    public CombinedResult apply(MockState state) {
        List<?> results = commands.stream()
                .map(cmd -> cmd.apply(state))
                .collect(Collectors.toList());
        return new CombinedResult(results);
    }

}
