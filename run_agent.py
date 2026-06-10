from agent.graph import DataQualityAgent

def main():
    # Initialize the agent
    agent = DataQualityAgent()

    # Simulate a NULL Explosion failure on HISTORICAL_STOCK
    run_id = agent.run(
        failure_type="dbt_test",
        table_name="HISTORICAL_STOCK",
        failure_details="not_null check failed on column close_price. 143 rows affected.",
        pipeline_name="stock_market_batch_pipeline"
    )

    # At this point the agent is paused at HITL
    # Now we resume it with approval
    print("\n" + "="*50)
    print("Resuming agent with approval...")
    print("="*50 + "\n")

    agent.resume(
        run_id=run_id,
        decision="approved",
        feedback="Looks good, apply the fix"
    )

if __name__ == "__main__":
    main()