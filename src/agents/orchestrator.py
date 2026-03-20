class AgentOrchestrator:
    def __init__(self, agents=None):
        self.agents = list(agents or [])

    async def run(self, context):
        working = dict(context or {})
        for agent in self.agents:
            working = await agent.process(working)
            if working is None:
                return {}
            if working.get("halt_pipeline"):
                break
        return working
