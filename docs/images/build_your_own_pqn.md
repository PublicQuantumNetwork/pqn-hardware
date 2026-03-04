# How to build your own PQN

Building a public quantum network occurs in three main phases:
1. Form your team. Decide on where you’d like the entanglement source and public node to be (should be within a ~5 mile radius). Reach out to: the public node building administration to gauge interest; relevant optical fiber companies / organizations to determine what fibers are available between the nodes; researchers familiar with quantum optics to help set up the network components. 
2. Build and test hardware. Connect fiber links and characterize them. Typical values are below. Assemble and install your components. Below are the bill of materials and general setup procedures.
3. Launch and maintain the public quantum network. Prepare a launch event and ongoing outreach activiites for the public. Share your updates, modifications, and outreach materials with the overall PQN effort.

## Bill of Materials
Below are estimates; we are constantly working to reduce the cost of components.
| Components | Price |
|------------|------------|
| General Supplies | $256 |
| Public Interface | $1,137 | 
| Rack-mountable Measurement System for Network Closet | $3,643 | 
| Entanglement Source | $56,164 | 
| Detectors | $30,000 | 
| Total | $91,200 |

## Characterizing Fiber Links
*We use dark fibers (fibers not being used for data). We are studying how to send quantum signals over fibers already in use for classical communication (so-called quantum-classical co-existence).
*Fiber-related costs may include a non-recurring engineering fee to get the dark fiber from the lab to the public space (~$20k typical) and fees for continued use.
*Typical loss and dark counts for 24.3-km roundtrip fiber: ~10-dB loss at 1550 nm and ~1,500 cps dark counts 
*Typically, polarization drift requires daily compensation; we are working on automating this.

## Setting up PQN Hardware

There are three main parts to PQN hardware: the entanglement source, the public interface, and the measurement system.

Telecom photons from the entanglement source are sent through the fiber loop to the public node. At the public node, the user chooses measurement settings using the public interface. These settings are applied to a measurement system inside the network closet through which the photons pass.

## Setting up PQN Software

The PQN software and installation instructions are provided on the PQN Github: https://github.com/PublicQuantumNetwork


