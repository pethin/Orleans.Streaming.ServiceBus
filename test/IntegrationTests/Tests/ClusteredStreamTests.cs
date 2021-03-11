using System;
using System.Threading.Tasks;
using IntegrationTests.Grains;
using Xunit;

namespace IntegrationTests.Tests
{
    public class ClusteredStreamTests : TestBase
    {
        public ClusteredStreamTests()
        {
            Initialize(3);
        }
        
        [Fact]
        public async Task ProduceConsumeTest()
        {
            var grain = await ActivateGrain<ILoopbackGrain>();
            var result = await grain.SendTestEvent(TimeSpan.FromSeconds(5));

            Assert.NotNull(result);
            Assert.Equal(result!.Expected, result.Actual);
        }
        
        private async Task<TGrain> ActivateGrain<TGrain>() where TGrain : IBaseTestGrain
        {
            var grain = Cluster!.GrainFactory.GetGrain<TGrain>(Guid.NewGuid());
            await grain.Initialize();
            return grain;
        }
    }
}